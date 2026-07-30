--------------------------- MODULE MpscReservation ---------------------------
EXTENDS Naturals, FiniteSets

(***************************************************************************
 * A finite, strictly ordered MPSC reservation model.  Logical positions are
 * not reused in this small model; slot sequence reuse is orthogonal to the
 * owner-death hole recovery protocol modeled here.
 ***************************************************************************)

Producers == {"producer-0", "producer-1"}
Positions == 0..2
LastPosition == 2
EndPosition == LastPosition + 1
MaxEpoch == 2
NoOwner == "NO_OWNER"

SlotStates == {"FREE", "RESERVED", "WRITING", "READY", "ABORTED", "RETIRED"}
ReservationStates == {"RESERVED", "WRITING"}

VARIABLES slotState,
          slotOwner,
          slotOwnerEpoch,
          payloadWritten,
          producerLive,
          producerEpoch,
          producerTail,
          consumerHead

vars == <<slotState, slotOwner, slotOwnerEpoch, payloadWritten,
          producerLive, producerEpoch, producerTail, consumerHead>>

Init ==
    /\ slotState = [s \in Positions |-> "FREE"]
    /\ slotOwner = [s \in Positions |-> NoOwner]
    /\ slotOwnerEpoch = [s \in Positions |-> 0]
    /\ payloadWritten = [s \in Positions |-> FALSE]
    /\ producerLive = [p \in Producers |-> TRUE]
    /\ producerEpoch = [p \in Producers |-> 1]
    /\ producerTail = 0
    /\ consumerHead = 0

OwnerIncarnationIsCurrent(s) ==
    IF slotOwner[s] \in Producers
    THEN slotOwnerEpoch[s] = producerEpoch[slotOwner[s]]
    ELSE FALSE

ReservationOwnerIsLive(s) ==
    IF OwnerIncarnationIsCurrent(s)
    THEN producerLive[slotOwner[s]]
    ELSE FALSE

DeadReservation(s) ==
    /\ slotState[s] \in ReservationStates
    /\ ~ReservationOwnerIsLive(s)

Reserve(p) ==
    /\ p \in Producers
    /\ producerLive[p]
    /\ producerTail \in Positions
    /\ slotState[producerTail] = "FREE"
    /\ slotState' = [slotState EXCEPT ![producerTail] = "RESERVED"]
    /\ slotOwner' = [slotOwner EXCEPT ![producerTail] = p]
    /\ slotOwnerEpoch' = [slotOwnerEpoch EXCEPT ![producerTail] = producerEpoch[p]]
    /\ payloadWritten' = payloadWritten
    /\ producerTail' = producerTail + 1
    /\ UNCHANGED <<producerLive, producerEpoch, consumerHead>>

Write(p, s) ==
    /\ p \in Producers
    /\ s \in Positions
    /\ slotState[s] = "RESERVED"
    /\ slotOwner[s] = p
    /\ slotOwnerEpoch[s] = producerEpoch[p]
    /\ producerLive[p]
    /\ slotState' = [slotState EXCEPT ![s] = "WRITING"]
    /\ payloadWritten' = [payloadWritten EXCEPT ![s] = TRUE]
    /\ UNCHANGED <<slotOwner, slotOwnerEpoch, producerLive, producerEpoch,
                    producerTail, consumerHead>>

Commit(p, s) ==
    /\ p \in Producers
    /\ s \in Positions
    /\ slotState[s] = "WRITING"
    /\ payloadWritten[s]
    /\ slotOwner[s] = p
    /\ slotOwnerEpoch[s] = producerEpoch[p]
    /\ producerLive[p]
    /\ slotState' = [slotState EXCEPT ![s] = "READY"]
    /\ UNCHANGED <<slotOwner, slotOwnerEpoch, payloadWritten, producerLive,
                    producerEpoch, producerTail, consumerHead>>

OwnerDeath(p) ==
    /\ p \in Producers
    /\ producerLive[p]
    /\ producerLive' = [producerLive EXCEPT ![p] = FALSE]
    /\ UNCHANGED <<slotState, slotOwner, slotOwnerEpoch, payloadWritten,
                    producerEpoch, producerTail, consumerHead>>

(***************************************************************************
 * Restart models PID/publisher identity reuse.  Reservations retain the old
 * epoch, so a restarted producer cannot commit an old reservation.
 ***************************************************************************)
OwnerRestart(p) ==
    /\ p \in Producers
    /\ ~producerLive[p]
    /\ producerEpoch[p] < MaxEpoch
    /\ producerEpoch' = [producerEpoch EXCEPT ![p] = @ + 1]
    /\ producerLive' = [producerLive EXCEPT ![p] = TRUE]
    /\ UNCHANGED <<slotState, slotOwner, slotOwnerEpoch, payloadWritten,
                    producerTail, consumerHead>>

RecoveryAbort(s) ==
    /\ s \in Positions
    /\ DeadReservation(s)
    /\ slotState' = [slotState EXCEPT ![s] = "ABORTED"]
    /\ UNCHANGED <<slotOwner, slotOwnerEpoch, payloadWritten, producerLive,
                    producerEpoch, producerTail, consumerHead>>

ConsumeReady(s) ==
    /\ s \in Positions
    /\ consumerHead = s
    /\ slotState[s] = "READY"
    /\ slotState' = [slotState EXCEPT ![s] = "RETIRED"]
    /\ consumerHead' = consumerHead + 1
    /\ UNCHANGED <<slotOwner, slotOwnerEpoch, payloadWritten, producerLive,
                    producerEpoch, producerTail>>

ConsumeAborted(s) ==
    /\ s \in Positions
    /\ consumerHead = s
    /\ slotState[s] = "ABORTED"
    /\ slotState' = [slotState EXCEPT ![s] = "RETIRED"]
    /\ consumerHead' = consumerHead + 1
    /\ UNCHANGED <<slotOwner, slotOwnerEpoch, payloadWritten, producerLive,
                    producerEpoch, producerTail>>

Next ==
    \/ \E p \in Producers : Reserve(p)
    \/ \E p \in Producers, s \in Positions : Write(p, s)
    \/ \E p \in Producers, s \in Positions : Commit(p, s)
    \/ \E p \in Producers : OwnerDeath(p)
    \/ \E p \in Producers : OwnerRestart(p)
    \/ \E s \in Positions : RecoveryAbort(s)
    \/ \E s \in Positions : ConsumeReady(s)
    \/ \E s \in Positions : ConsumeAborted(s)

RecoveryFairness ==
    /\ \A s \in Positions : WF_vars(RecoveryAbort(s))
    /\ \A s \in Positions : WF_vars(ConsumeAborted(s))
    /\ \A s \in Positions : WF_vars(ConsumeReady(s))

Spec == Init /\ [][Next]_vars /\ RecoveryFairness

TypeOK ==
    /\ slotState \in [Positions -> SlotStates]
    /\ slotOwner \in [Positions -> (Producers \cup {NoOwner})]
    /\ slotOwnerEpoch \in [Positions -> 0..MaxEpoch]
    /\ payloadWritten \in [Positions -> BOOLEAN]
    /\ producerLive \in [Producers -> BOOLEAN]
    /\ producerEpoch \in [Producers -> 1..MaxEpoch]
    /\ producerTail \in 0..EndPosition
    /\ consumerHead \in 0..EndPosition

TailIsContiguous ==
    \A s \in Positions : (s < producerTail) <=> (slotState[s] # "FREE")

ConsumedPrefixClosed ==
    \A s \in Positions : s < consumerHead => slotState[s] = "RETIRED"

ConsumerNeverPassesTail == consumerHead <= producerTail

ReservationOwnerBinding ==
    \A s \in Positions :
        slotState[s] # "FREE" =>
            IF slotOwner[s] \in Producers
            THEN slotOwnerEpoch[s] \in 1..producerEpoch[slotOwner[s]]
            ELSE FALSE

ReadyImpliesCompleteWrite ==
    \A s \in Positions : slotState[s] = "READY" => payloadWritten[s]

DeadReservationIsNeverConsumedAsData ==
    \A s \in Positions : DeadReservation(s) => slotState[s] # "READY"

(***************************************************************************
 * Safety side of INV-17: a dead owner at the strict-order head is an
 * explicit recovery candidate, and the consumer has not skipped the hole.
 ***************************************************************************)
DeadHeadIsRecoverable ==
    IF consumerHead \in Positions
    THEN DeadReservation(consumerHead) =>
            /\ ENABLED RecoveryAbort(consumerHead)
            /\ \A s \in Positions :
                   s < consumerHead => slotState[s] = "RETIRED"
    ELSE TRUE

(***************************************************************************
 * Liveness side of INV-17 under RecoveryFairness.  It is intentionally a
 * PROPERTY, not mislabeled as a state invariant.
 ***************************************************************************)
DeadReservationEventuallyAborted ==
    \A s \in Positions :
        [](DeadReservation(s) => <>(slotState[s] \in {"ABORTED", "RETIRED"}))

QueueNotPermanentlyBlockedByDeadOwner ==
    \A s \in Positions :
        []((consumerHead = s /\ DeadReservation(s)) => <>(consumerHead > s))

=============================================================================
