------------------------- MODULE BroadcastMembership ------------------------
EXTENDS Naturals, FiniteSets

(***************************************************************************
 * Broadcast membership and generation-scoped ACK responsibility.  ACK bits
 * are represented as subscriber tokens (id, generation), making the identity
 * that accompanies each conceptual bitmap bit explicit.
 ***************************************************************************)

SubscriberIds == {"subscriber-0"}
Messages == 0..1
Generations == 0..2
MaxGeneration == 2

Token(i, g) == [id |-> i, generation |-> g]
TokenSet == {Token(i, g) : i \in SubscriberIds, g \in Generations}

VARIABLES active,
          borrowAllowed,
          generation,
          membershipVersion,
          published,
          snapshotVersion,
          snapshot,
          ackDue,
          acked,
          cleaned,
          retired,
          pendingCleanup,
          cleanupHistory

vars == <<active, borrowAllowed, generation, membershipVersion, published,
          snapshotVersion, snapshot, ackDue, acked, cleaned, retired,
          pendingCleanup, cleanupHistory>>

ActiveIds == {i \in SubscriberIds : active[i]}
CurrentMembers == {Token(i, generation[i]) : i \in ActiveIds}

Init ==
    /\ active = [i \in SubscriberIds |-> FALSE]
    /\ borrowAllowed = [i \in SubscriberIds |-> FALSE]
    /\ generation = [i \in SubscriberIds |-> 0]
    /\ membershipVersion = 0
    /\ published = [m \in Messages |-> FALSE]
    /\ snapshotVersion = [m \in Messages |-> 0]
    /\ snapshot = [m \in Messages |-> {}]
    /\ ackDue = [m \in Messages |-> {}]
    /\ acked = [m \in Messages |-> {}]
    /\ cleaned = [m \in Messages |-> {}]
    /\ retired = [m \in Messages |-> FALSE]
    /\ pendingCleanup = {}
    /\ cleanupHistory = {}

Register(i) ==
    /\ i \in SubscriberIds
    /\ ~active[i]
    /\ generation[i] < MaxGeneration
    /\ generation' = [generation EXCEPT ![i] = @ + 1]
    /\ active' = [active EXCEPT ![i] = TRUE]
    /\ borrowAllowed' = [borrowAllowed EXCEPT ![i] = TRUE]
    /\ membershipVersion' = membershipVersion + 1
    /\ UNCHANGED <<published, snapshotVersion, snapshot, ackDue, acked,
                    cleaned, retired, pendingCleanup, cleanupHistory>>

(***************************************************************************
 * Unregister first prevents new borrows, then queues cleanup for exactly the
 * generation being removed.  Re-registration may race ahead in this model;
 * exact-token cleanup must therefore remain safe even after ID reuse.
 ***************************************************************************)
Unregister(i) ==
    /\ i \in SubscriberIds
    /\ active[i]
    /\ LET oldToken == Token(i, generation[i]) IN
       /\ active' = [active EXCEPT ![i] = FALSE]
       /\ borrowAllowed' = [borrowAllowed EXCEPT ![i] = FALSE]
       /\ pendingCleanup' = pendingCleanup \cup {oldToken}
    /\ membershipVersion' = membershipVersion + 1
    /\ UNCHANGED <<generation, published, snapshotVersion, snapshot, ackDue,
                    acked, cleaned, retired, cleanupHistory>>

Publish(m) ==
    /\ m \in Messages
    /\ ~published[m]
    /\ published' = [published EXCEPT ![m] = TRUE]
    /\ snapshotVersion' = [snapshotVersion EXCEPT ![m] = membershipVersion]
    /\ snapshot' = [snapshot EXCEPT ![m] = CurrentMembers]
    /\ ackDue' = [ackDue EXCEPT ![m] = CurrentMembers]
    /\ acked' = [acked EXCEPT ![m] = {}]
    /\ cleaned' = [cleaned EXCEPT ![m] = {}]
    /\ UNCHANGED <<active, borrowAllowed, generation, membershipVersion,
                    retired, pendingCleanup, cleanupHistory>>

Ack(m, t) ==
    /\ m \in Messages
    /\ t \in TokenSet
    /\ published[m]
    /\ ~retired[m]
    /\ t \in ackDue[m]
    /\ ackDue' = [ackDue EXCEPT ![m] = @ \ {t}]
    /\ acked' = [acked EXCEPT ![m] = @ \cup {t}]
    /\ UNCHANGED <<active, borrowAllowed, generation, membershipVersion,
                    published, snapshotVersion, snapshot, cleaned, retired,
                    pendingCleanup, cleanupHistory>>

Cleanup(t) ==
    /\ t \in pendingCleanup
    /\ ackDue' = [m \in Messages |-> ackDue[m] \ {t}]
    /\ cleaned' = [m \in Messages |->
                       IF t \in snapshot[m]
                       THEN cleaned[m] \cup {t}
                       ELSE cleaned[m]]
    /\ pendingCleanup' = pendingCleanup \ {t}
    /\ cleanupHistory' = cleanupHistory \cup {t}
    /\ UNCHANGED <<active, borrowAllowed, generation, membershipVersion,
                    published, snapshotVersion, snapshot, acked, retired>>

Retire(m) ==
    /\ m \in Messages
    /\ published[m]
    /\ ~retired[m]
    /\ ackDue[m] = {}
    /\ retired' = [retired EXCEPT ![m] = TRUE]
    /\ UNCHANGED <<active, borrowAllowed, generation, membershipVersion,
                    published, snapshotVersion, snapshot, ackDue, acked,
                    cleaned, pendingCleanup, cleanupHistory>>

Next ==
    \/ \E i \in SubscriberIds : Register(i)
    \/ \E i \in SubscriberIds : Unregister(i)
    \/ \E m \in Messages : Publish(m)
    \/ \E m \in Messages, t \in TokenSet : Ack(m, t)
    \/ \E t \in TokenSet : Cleanup(t)
    \/ \E m \in Messages : Retire(m)

CleanupFairness ==
    /\ \A t \in TokenSet : WF_vars(Cleanup(t))
    /\ \A m \in Messages, t \in TokenSet : WF_vars(Ack(m, t))
    /\ \A m \in Messages : WF_vars(Retire(m))

Spec == Init /\ [][Next]_vars /\ CleanupFairness

TypeOK ==
    /\ active \in [SubscriberIds -> BOOLEAN]
    /\ borrowAllowed \in [SubscriberIds -> BOOLEAN]
    /\ generation \in [SubscriberIds -> Generations]
    /\ membershipVersion \in Nat
    /\ published \in [Messages -> BOOLEAN]
    /\ snapshotVersion \in [Messages -> Nat]
    /\ snapshot \in [Messages -> SUBSET TokenSet]
    /\ ackDue \in [Messages -> SUBSET TokenSet]
    /\ acked \in [Messages -> SUBSET TokenSet]
    /\ cleaned \in [Messages -> SUBSET TokenSet]
    /\ retired \in [Messages -> BOOLEAN]
    /\ pendingCleanup \subseteq TokenSet
    /\ cleanupHistory \subseteq TokenSet

MembershipStateOK ==
    \A i \in SubscriberIds :
        /\ (active[i] <=> borrowAllowed[i])
        /\ (active[i] => generation[i] > 0)

PublishedStateWellFormed ==
    \A m \in Messages :
        /\ (~published[m] =>
               /\ snapshot[m] = {}
               /\ ackDue[m] = {}
               /\ acked[m] = {}
               /\ cleaned[m] = {}
               /\ ~retired[m])
        /\ (published[m] => snapshotVersion[m] <= membershipVersion)

SnapshotHasOneGenerationPerId ==
    \A m \in Messages, i \in SubscriberIds,
       g1 \in Generations, g2 \in Generations :
        Token(i, g1) \in snapshot[m] /\ Token(i, g2) \in snapshot[m] => g1 = g2

AckAccounting ==
    \A m \in Messages :
        /\ acked[m] \subseteq snapshot[m]
        /\ cleaned[m] \subseteq snapshot[m]
        /\ ackDue[m] = snapshot[m] \ (acked[m] \cup cleaned[m])

GenerationScopedCleanup ==
    /\ \A m \in Messages : cleaned[m] \subseteq cleanupHistory
    /\ \A t \in cleanupHistory : t \notin pendingCleanup

RetiredOnlyAfterAllResponsibilitiesClear ==
    \A m \in Messages : retired[m] => published[m] /\ ackDue[m] = {}

(***************************************************************************
 * INV-18: completion by an old (id, generation) token cannot clear an
 * uncompleted responsibility carried by a different generation of that ID.
 ***************************************************************************)
OldGenerationDoesNotClearNewResponsibility ==
    \A m \in Messages, i \in SubscriberIds,
       oldGen \in Generations, newGen \in Generations :
        /\ oldGen # newGen
        /\ Token(i, oldGen) \in cleanupHistory
        /\ Token(i, newGen) \in snapshot[m]
        /\ Token(i, newGen) \notin acked[m]
        /\ Token(i, newGen) \notin cleanupHistory
        => Token(i, newGen) \in ackDue[m]

PendingCleanupEventuallyCompletes ==
    \A t \in TokenSet : [](t \in pendingCleanup => <>(t \notin pendingCleanup))

PublishedMessagesEventuallyRetire ==
    \A m \in Messages : [](published[m] => <>retired[m])

=============================================================================
