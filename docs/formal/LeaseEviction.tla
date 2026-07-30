---------------------------- MODULE LeaseEviction ----------------------------
EXTENDS Naturals, FiniteSets

(***************************************************************************
 * Subscriber lease eviction with expiry observation, process-liveness
 * recheck, generation/lease-epoch binding, and generation-scoped ACK/Pin
 * cleanup.  Owner identities are process incarnations: once crashed, an
 * identity does not become live again; reuse selects another identity.
 ***************************************************************************)

SubscriberIds == {"subscriber-0", "subscriber-1"}
Owners == {"process-0", "process-1"}
Generations == 1..3
LeaseEpochs == 1..3
MaxGeneration == 3
MaxLeaseEpoch == 3
Times == 0..5
MaxTime == 5
TTL == 2

LeaseStates == {"ACTIVE", "EXPIRED", "RECHECKED", "EVICTING", "EVICTED"}
AckResources == {"ack-0", "ack-1"}
PinResources == {"pin-0", "pin-1"}
NoLease ==
    [subscriber |-> "NO_SUBSCRIBER", generation |-> 0,
     leaseEpoch |-> 0, owner |-> "NO_OWNER"]

LeaseToken(i, g, e, o) ==
    [subscriber |-> i, generation |-> g, leaseEpoch |-> e, owner |-> o]

LeaseTokens ==
    {LeaseToken(i, g, e, o) :
        i \in SubscriberIds, g \in Generations,
        e \in LeaseEpochs, o \in Owners}

AckShare(r, lease) == [resource |-> r, lease |-> lease]
PinShare(r, lease) == [resource |-> r, lease |-> lease]

AckShareSet == {AckShare(r, lease) : r \in AckResources, lease \in LeaseTokens}
PinShareSet == {PinShare(r, lease) : r \in PinResources, lease \in LeaseTokens}

InitialOwner(i) == IF i = "subscriber-0" THEN "process-0" ELSE "process-1"
InitialLease(i) == LeaseToken(i, 1, 1, InitialOwner(i))

VARIABLES now,
          ownerLive,
          leaseState,
          owner,
          generation,
          leaseEpoch,
          heartbeat,
          issuedLeases,
          recheckedLease,
          evictionLease,
          ackShares,
          pinShares,
          ackCleaned,
          pinCleaned,
          ackCleanupHistory,
          pinCleanupHistory

vars == <<now, ownerLive, leaseState, owner, generation, leaseEpoch,
          heartbeat, issuedLeases, recheckedLease, evictionLease, ackShares,
          pinShares, ackCleaned, pinCleaned, ackCleanupHistory,
          pinCleanupHistory>>

CurrentLease(i) == LeaseToken(i, generation[i], leaseEpoch[i], owner[i])

Init ==
    /\ now = 0
    /\ ownerLive = [o \in Owners |-> TRUE]
    /\ leaseState = [i \in SubscriberIds |-> "ACTIVE"]
    /\ owner = [i \in SubscriberIds |-> InitialOwner(i)]
    /\ generation = [i \in SubscriberIds |-> 1]
    /\ leaseEpoch = [i \in SubscriberIds |-> 1]
    /\ heartbeat = [i \in SubscriberIds |-> 0]
    /\ issuedLeases = {InitialLease(i) : i \in SubscriberIds}
    /\ recheckedLease = [i \in SubscriberIds |-> NoLease]
    /\ evictionLease = [i \in SubscriberIds |-> NoLease]
    /\ ackShares = {AckShare("ack-0", InitialLease("subscriber-0")),
                     AckShare("ack-1", InitialLease("subscriber-1"))}
    /\ pinShares = {PinShare("pin-0", InitialLease("subscriber-0")),
                     PinShare("pin-1", InitialLease("subscriber-1"))}
    /\ ackCleaned = [i \in SubscriberIds |-> FALSE]
    /\ pinCleaned = [i \in SubscriberIds |-> FALSE]
    /\ ackCleanupHistory = {}
    /\ pinCleanupHistory = {}

Tick ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<ownerLive, leaseState, owner, generation, leaseEpoch,
                    heartbeat, issuedLeases, recheckedLease, evictionLease,
                    ackShares, pinShares, ackCleaned, pinCleaned,
                    ackCleanupHistory, pinCleanupHistory>>

Heartbeat(i) ==
    /\ i \in SubscriberIds
    /\ leaseState[i] = "ACTIVE"
    /\ ownerLive[owner[i]]
    /\ heartbeat' = [heartbeat EXCEPT ![i] = now]
    /\ UNCHANGED <<now, ownerLive, leaseState, owner, generation, leaseEpoch,
                    issuedLeases, recheckedLease, evictionLease, ackShares,
                    pinShares, ackCleaned, pinCleaned, ackCleanupHistory,
                    pinCleanupHistory>>

OwnerCrash(o) ==
    /\ o \in Owners
    /\ ownerLive[o]
    /\ ownerLive' = [ownerLive EXCEPT ![o] = FALSE]
    /\ UNCHANGED <<now, leaseState, owner, generation, leaseEpoch, heartbeat,
                    issuedLeases, recheckedLease, evictionLease, ackShares,
                    pinShares, ackCleaned, pinCleaned, ackCleanupHistory,
                    pinCleanupHistory>>

ObserveExpiry(i) ==
    /\ i \in SubscriberIds
    /\ leaseState[i] = "ACTIVE"
    /\ now >= heartbeat[i] + TTL
    /\ leaseState' = [leaseState EXCEPT ![i] = "EXPIRED"]
    /\ UNCHANGED <<now, ownerLive, owner, generation, leaseEpoch, heartbeat,
                    issuedLeases, recheckedLease, evictionLease, ackShares,
                    pinShares, ackCleaned, pinCleaned, ackCleanupHistory,
                    pinCleanupHistory>>

(***************************************************************************
 * Expiry is only a suspicion.  A successful liveness recheck returns the
 * lease to ACTIVE rather than evicting a paused-but-live owner.
 ***************************************************************************)
LivenessRecheckAlive(i) ==
    /\ i \in SubscriberIds
    /\ leaseState[i] = "EXPIRED"
    /\ ownerLive[owner[i]]
    /\ leaseState' = [leaseState EXCEPT ![i] = "ACTIVE"]
    /\ heartbeat' = [heartbeat EXCEPT ![i] = now]
    /\ UNCHANGED <<now, ownerLive, owner, generation, leaseEpoch, issuedLeases,
                    recheckedLease, evictionLease, ackShares, pinShares,
                    ackCleaned, pinCleaned, ackCleanupHistory,
                    pinCleanupHistory>>

LivenessRecheckDead(i) ==
    /\ i \in SubscriberIds
    /\ leaseState[i] = "EXPIRED"
    /\ ~ownerLive[owner[i]]
    /\ leaseState' = [leaseState EXCEPT ![i] = "RECHECKED"]
    /\ recheckedLease' = [recheckedLease EXCEPT ![i] = CurrentLease(i)]
    /\ UNCHANGED <<now, ownerLive, owner, generation, leaseEpoch, heartbeat,
                    issuedLeases, evictionLease, ackShares, pinShares,
                    ackCleaned, pinCleaned, ackCleanupHistory,
                    pinCleanupHistory>>

BeginEviction(i) ==
    /\ i \in SubscriberIds
    /\ leaseState[i] = "RECHECKED"
    /\ recheckedLease[i] = CurrentLease(i)
    /\ ~ownerLive[owner[i]]
    /\ leaseState' = [leaseState EXCEPT ![i] = "EVICTING"]
    /\ evictionLease' = [evictionLease EXCEPT ![i] = CurrentLease(i)]
    /\ ackCleaned' = [ackCleaned EXCEPT ![i] = FALSE]
    /\ pinCleaned' = [pinCleaned EXCEPT ![i] = FALSE]
    /\ UNCHANGED <<now, ownerLive, owner, generation, leaseEpoch, heartbeat,
                    issuedLeases, recheckedLease, ackShares, pinShares,
                    ackCleanupHistory, pinCleanupHistory>>

CleanupAcks(i) ==
    /\ i \in SubscriberIds
    /\ leaseState[i] = "EVICTING"
    /\ evictionLease[i] = CurrentLease(i)
    /\ ~ackCleaned[i]
    /\ ackShares' = {share \in ackShares : share.lease # evictionLease[i]}
    /\ ackCleaned' = [ackCleaned EXCEPT ![i] = TRUE]
    /\ ackCleanupHistory' = ackCleanupHistory \cup {evictionLease[i]}
    /\ UNCHANGED <<now, ownerLive, leaseState, owner, generation, leaseEpoch,
                    heartbeat, issuedLeases, recheckedLease, evictionLease,
                    pinShares, pinCleaned, pinCleanupHistory>>

CleanupPins(i) ==
    /\ i \in SubscriberIds
    /\ leaseState[i] = "EVICTING"
    /\ evictionLease[i] = CurrentLease(i)
    /\ ~pinCleaned[i]
    /\ pinShares' = {share \in pinShares : share.lease # evictionLease[i]}
    /\ pinCleaned' = [pinCleaned EXCEPT ![i] = TRUE]
    /\ pinCleanupHistory' = pinCleanupHistory \cup {evictionLease[i]}
    /\ UNCHANGED <<now, ownerLive, leaseState, owner, generation, leaseEpoch,
                    heartbeat, issuedLeases, recheckedLease, evictionLease,
                    ackShares, ackCleaned, ackCleanupHistory>>

FinishEviction(i) ==
    /\ i \in SubscriberIds
    /\ leaseState[i] = "EVICTING"
    /\ evictionLease[i] = CurrentLease(i)
    /\ ackCleaned[i]
    /\ pinCleaned[i]
    /\ ~\E share \in ackShares : share.lease = evictionLease[i]
    /\ ~\E share \in pinShares : share.lease = evictionLease[i]
    /\ leaseState' = [leaseState EXCEPT ![i] = "EVICTED"]
    /\ UNCHANGED <<now, ownerLive, owner, generation, leaseEpoch, heartbeat,
                    issuedLeases, recheckedLease, evictionLease, ackShares,
                    pinShares, ackCleaned, pinCleaned, ackCleanupHistory,
                    pinCleanupHistory>>

(***************************************************************************
 * ID reuse creates a new generation and lease epoch.  Old cleanup history
 * remains present so invariants can detect an ABA-style cross-generation
 * cleanup if one is introduced.
 ***************************************************************************)
ReuseLease(i, o) ==
    /\ i \in SubscriberIds
    /\ o \in Owners
    /\ leaseState[i] = "EVICTED"
    /\ ownerLive[o]
    /\ generation[i] < MaxGeneration
    /\ leaseEpoch[i] < MaxLeaseEpoch
    /\ LET newLease == LeaseToken(i, generation[i] + 1, leaseEpoch[i] + 1, o) IN
       /\ leaseState' = [leaseState EXCEPT ![i] = "ACTIVE"]
       /\ owner' = [owner EXCEPT ![i] = o]
       /\ generation' = [generation EXCEPT ![i] = @ + 1]
       /\ leaseEpoch' = [leaseEpoch EXCEPT ![i] = @ + 1]
       /\ heartbeat' = [heartbeat EXCEPT ![i] = now]
       /\ issuedLeases' = issuedLeases \cup {newLease}
       /\ recheckedLease' = [recheckedLease EXCEPT ![i] = NoLease]
       /\ evictionLease' = [evictionLease EXCEPT ![i] = NoLease]
       /\ ackCleaned' = [ackCleaned EXCEPT ![i] = FALSE]
       /\ pinCleaned' = [pinCleaned EXCEPT ![i] = FALSE]
    /\ UNCHANGED <<now, ownerLive, ackShares, pinShares, ackCleanupHistory,
                    pinCleanupHistory>>

AddAckResponsibility(i, r) ==
    /\ i \in SubscriberIds
    /\ r \in AckResources
    /\ leaseState[i] = "ACTIVE"
    /\ ownerLive[owner[i]]
    /\ ackShares' = ackShares \cup {AckShare(r, CurrentLease(i))}
    /\ UNCHANGED <<now, ownerLive, leaseState, owner, generation, leaseEpoch,
                    heartbeat, issuedLeases, recheckedLease, evictionLease,
                    pinShares, ackCleaned, pinCleaned, ackCleanupHistory,
                    pinCleanupHistory>>

AddPin(i, r) ==
    /\ i \in SubscriberIds
    /\ r \in PinResources
    /\ leaseState[i] = "ACTIVE"
    /\ ownerLive[owner[i]]
    /\ pinShares' = pinShares \cup {PinShare(r, CurrentLease(i))}
    /\ UNCHANGED <<now, ownerLive, leaseState, owner, generation, leaseEpoch,
                    heartbeat, issuedLeases, recheckedLease, evictionLease,
                    ackShares, ackCleaned, pinCleaned, ackCleanupHistory,
                    pinCleanupHistory>>

Next ==
    \/ Tick
    \/ \E i \in SubscriberIds : Heartbeat(i)
    \/ \E o \in Owners : OwnerCrash(o)
    \/ \E i \in SubscriberIds : ObserveExpiry(i)
    \/ \E i \in SubscriberIds : LivenessRecheckAlive(i)
    \/ \E i \in SubscriberIds : LivenessRecheckDead(i)
    \/ \E i \in SubscriberIds : BeginEviction(i)
    \/ \E i \in SubscriberIds : CleanupAcks(i)
    \/ \E i \in SubscriberIds : CleanupPins(i)
    \/ \E i \in SubscriberIds : FinishEviction(i)
    \/ \E i \in SubscriberIds, o \in Owners : ReuseLease(i, o)
    \/ \E i \in SubscriberIds, r \in AckResources : AddAckResponsibility(i, r)
    \/ \E i \in SubscriberIds, r \in PinResources : AddPin(i, r)

EvictionFairness ==
    /\ \A i \in SubscriberIds : WF_vars(LivenessRecheckAlive(i))
    /\ \A i \in SubscriberIds : WF_vars(LivenessRecheckDead(i))
    /\ \A i \in SubscriberIds : WF_vars(BeginEviction(i))
    /\ \A i \in SubscriberIds : WF_vars(CleanupAcks(i))
    /\ \A i \in SubscriberIds : WF_vars(CleanupPins(i))
    /\ \A i \in SubscriberIds : WF_vars(FinishEviction(i))

Spec == Init /\ [][Next]_vars /\ EvictionFairness

TypeOK ==
    /\ now \in Times
    /\ ownerLive \in [Owners -> BOOLEAN]
    /\ leaseState \in [SubscriberIds -> LeaseStates]
    /\ owner \in [SubscriberIds -> Owners]
    /\ generation \in [SubscriberIds -> Generations]
    /\ leaseEpoch \in [SubscriberIds -> LeaseEpochs]
    /\ heartbeat \in [SubscriberIds -> Times]
    /\ issuedLeases \subseteq LeaseTokens
    /\ recheckedLease \in [SubscriberIds -> (LeaseTokens \cup {NoLease})]
    /\ evictionLease \in [SubscriberIds -> (LeaseTokens \cup {NoLease})]
    /\ ackShares \subseteq AckShareSet
    /\ pinShares \subseteq PinShareSet
    /\ ackCleaned \in [SubscriberIds -> BOOLEAN]
    /\ pinCleaned \in [SubscriberIds -> BOOLEAN]
    /\ ackCleanupHistory \subseteq LeaseTokens
    /\ pinCleanupHistory \subseteq LeaseTokens

CurrentLeaseWasIssued ==
    \A i \in SubscriberIds : CurrentLease(i) \in issuedLeases

(***************************************************************************
 * For one subscriber ID, a generation denotes exactly one lease epoch and
 * owner incarnation.  Cleanup comparisons still use the full lease token.
 ***************************************************************************)
GenerationLeaseEpochBinding ==
    \A a \in issuedLeases, b \in issuedLeases :
        a.subscriber = b.subscriber /\ a.generation = b.generation =>
            /\ a.leaseEpoch = b.leaseEpoch
            /\ a.owner = b.owner

RecheckBindsCurrentLease ==
    \A i \in SubscriberIds :
        leaseState[i] = "RECHECKED" =>
            /\ recheckedLease[i] = CurrentLease(i)
            /\ ~ownerLive[owner[i]]

EvictionBindsCurrentLease ==
    \A i \in SubscriberIds :
        leaseState[i] \in {"EVICTING", "EVICTED"} =>
            /\ evictionLease[i] = CurrentLease(i)
            /\ ~ownerLive[owner[i]]

LiveOwnerIsNeverEvicted ==
    \A i \in SubscriberIds :
        ownerLive[owner[i]] => leaseState[i] \notin {"EVICTING", "EVICTED"}

CleanupIsLeaseScoped ==
    /\ \A share \in ackShares : share.lease \notin ackCleanupHistory
    /\ \A share \in pinShares : share.lease \notin pinCleanupHistory
    /\ \A lease \in ackCleanupHistory : lease \in issuedLeases
    /\ \A lease \in pinCleanupHistory : lease \in issuedLeases

OldLeaseCleanupDoesNotAffectCurrentLease ==
    \A i \in SubscriberIds :
        leaseState[i] = "ACTIVE" =>
            /\ CurrentLease(i) \notin ackCleanupHistory
            /\ CurrentLease(i) \notin pinCleanupHistory

EvictedHasNoResponsibilities ==
    \A i \in SubscriberIds :
        leaseState[i] = "EVICTED" =>
            /\ ~\E share \in ackShares : share.lease = CurrentLease(i)
            /\ ~\E share \in pinShares : share.lease = CurrentLease(i)
            /\ ackCleaned[i]
            /\ pinCleaned[i]

ExpiredDeadLeaseEventuallyEvicted ==
    \A i \in SubscriberIds :
        []((leaseState[i] = "EXPIRED" /\ ~ownerLive[owner[i]]) =>
           <>(leaseState[i] = "EVICTED"))

EvictingEventuallyEvicted ==
    \A i \in SubscriberIds :
        [](leaseState[i] = "EVICTING" => <>(leaseState[i] = "EVICTED"))

=============================================================================
