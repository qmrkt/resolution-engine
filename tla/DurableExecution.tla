--------------------------- MODULE DurableExecution ---------------------------
EXTENDS FiniteSets, Naturals, Sequences

(*
This model checks the single-node durable execution kernel at the level where
the Go runtime coordinates runs, workers, durable waiting state, resume signals,
timers, restart recovery, and callback delivery.

It intentionally abstracts away executor internals, CEL, JSON persistence, HTTP,
and full DAG evaluation. Each run has one abstract suspendable node. Once that
node is completed by a timer or signal resume, it is not run again in this model.
*)

CONSTANTS
    Runs,
    Workers,
    Signals,
    MaxQueueLen,
    None

ASSUME
    /\ Runs # {}
    /\ Workers # {}
    /\ MaxQueueLen > 0
    /\ None \notin Runs
    /\ None \notin Workers
    /\ None \notin Signals

Statuses == {"absent", "queued", "running", "waiting", "completed", "failed", "cancelled"}
TerminalStatuses == {"completed", "failed", "cancelled"}
WaitKinds == {"none", "timer", "signal"}

VARIABLES
    status,
    queue,
    workerRun,
    waitingKind,
    dueTimers,
    needsEnqueue,
    seenSignals,
    nodeDone,
    callbackPending,
    callbackDelivered

vars ==
    << status,
       queue,
       workerRun,
       waitingKind,
       dueTimers,
       needsEnqueue,
       seenSignals,
       nodeDone,
       callbackPending,
       callbackDelivered >>

SignalPairs == Runs \X Signals
ProcessingRuns == {workerRun[w] : w \in Workers} \ {None}
QueuedRuns == {r \in Runs : status[r] = "queued"}
WaitingRuns == {r \in Runs : status[r] = "waiting"}
TerminalRuns == {r \in Runs : status[r] \in TerminalStatuses}

ReleaseWorkerFor(run) ==
    [w \in Workers |-> IF workerRun[w] = run THEN None ELSE workerRun[w]]

TypeOK ==
    /\ status \in [Runs -> Statuses]
    /\ queue \in Seq(Runs)
    /\ Len(queue) <= MaxQueueLen
    /\ workerRun \in [Workers -> Runs \cup {None}]
    /\ waitingKind \in [Runs -> WaitKinds]
    /\ dueTimers \subseteq Runs
    /\ needsEnqueue \subseteq Runs
    /\ seenSignals \subseteq SignalPairs
    /\ nodeDone \subseteq Runs
    /\ callbackPending \subseteq Runs
    /\ callbackDelivered \subseteq Runs

NoDoubleProcessing ==
    \A w1, w2 \in Workers :
        /\ workerRun[w1] # None
        /\ workerRun[w1] = workerRun[w2]
        => w1 = w2

WorkerStatusConsistent ==
    \A w \in Workers :
        workerRun[w] # None => status[workerRun[w]] = "running"

RunningHasWorker ==
    \A r \in Runs :
        status[r] = "running" => r \in ProcessingRuns

WaitingDoesNotOccupyWorker ==
    \A r \in Runs :
        status[r] = "waiting" => r \notin ProcessingRuns

WaitingKindConsistent ==
    \A r \in Runs :
        status[r] = "waiting" <=> waitingKind[r] # "none"

QueuedStateClean ==
    \A r \in Runs :
        status[r] = "queued" =>
            /\ r \notin ProcessingRuns
            /\ waitingKind[r] = "none"

TerminalStateClean ==
    \A r \in Runs :
        status[r] \in TerminalStatuses =>
            /\ r \notin ProcessingRuns
            /\ waitingKind[r] = "none"
            /\ r \notin needsEnqueue
            /\ r \notin dueTimers

DueTimersOnlyWaitingTimers ==
    dueTimers \subseteq {r \in Runs : status[r] = "waiting" /\ waitingKind[r] = "timer"}

NeedsEnqueueOnlyQueued ==
    needsEnqueue \subseteq QueuedRuns

CompletedNodeIsNotWaiting ==
    nodeDone \cap WaitingRuns = {}

CallbacksOnlyAfterTerminal ==
    /\ callbackPending \subseteq TerminalRuns
    /\ callbackDelivered \subseteq TerminalRuns
    /\ callbackPending \cap callbackDelivered = {}

SafetyInvariants ==
    /\ NoDoubleProcessing
    /\ WorkerStatusConsistent
    /\ RunningHasWorker
    /\ WaitingDoesNotOccupyWorker
    /\ WaitingKindConsistent
    /\ QueuedStateClean
    /\ TerminalStateClean
    /\ DueTimersOnlyWaitingTimers
    /\ NeedsEnqueueOnlyQueued
    /\ CompletedNodeIsNotWaiting
    /\ CallbacksOnlyAfterTerminal

Submit(run) ==
    /\ status[run] = "absent"
    /\ Cardinality(QueuedRuns) < MaxQueueLen
    /\ status' = [status EXCEPT ![run] = "queued"]
    /\ needsEnqueue' = needsEnqueue \cup {run}
    /\ UNCHANGED
        << queue,
           workerRun,
           waitingKind,
           dueTimers,
           seenSignals,
           nodeDone,
           callbackPending,
           callbackDelivered >>

EnqueuePending(run) ==
    /\ run \in needsEnqueue
    /\ Len(queue) < MaxQueueLen
    /\ queue' = Append(queue, run)
    /\ needsEnqueue' = needsEnqueue \ {run}
    /\ UNCHANGED
        << status,
           workerRun,
           waitingKind,
           dueTimers,
           seenSignals,
           nodeDone,
           callbackPending,
           callbackDelivered >>

DuplicateEnqueue(run) ==
    /\ status[run] \in {"queued", "running", "waiting"}
    /\ Len(queue) < MaxQueueLen
    /\ queue' = Append(queue, run)
    /\ UNCHANGED
        << status,
           workerRun,
           waitingKind,
           dueTimers,
           needsEnqueue,
           seenSignals,
           nodeDone,
           callbackPending,
           callbackDelivered >>

Dequeue(worker) ==
    /\ workerRun[worker] = None
    /\ Len(queue) > 0
    /\ LET run == Head(queue)
       IN
        /\ queue' = Tail(queue)
        /\ IF status[run] = "queued" /\ run \notin ProcessingRuns
           THEN
            /\ status' = [status EXCEPT ![run] = "running"]
            /\ workerRun' = [workerRun EXCEPT ![worker] = run]
            /\ needsEnqueue' = needsEnqueue \ {run}
           ELSE
            /\ UNCHANGED << status, workerRun >>
            /\ UNCHANGED needsEnqueue
    /\ UNCHANGED
        << waitingKind,
           dueTimers,
           seenSignals,
           nodeDone,
           callbackPending,
           callbackDelivered >>

CompleteRun(worker) ==
    /\ workerRun[worker] # None
    /\ LET run == workerRun[worker]
       IN
        /\ status[run] = "running"
        /\ status' = [status EXCEPT ![run] = "completed"]
        /\ workerRun' = [workerRun EXCEPT ![worker] = None]
        /\ waitingKind' = [waitingKind EXCEPT ![run] = "none"]
        /\ dueTimers' = dueTimers \ {run}
        /\ needsEnqueue' = needsEnqueue \ {run}
        /\ nodeDone' = nodeDone \cup {run}
        /\ callbackPending' = callbackPending \cup {run}
    /\ UNCHANGED << queue, seenSignals, callbackDelivered >>

FailRun(worker) ==
    /\ workerRun[worker] # None
    /\ LET run == workerRun[worker]
       IN
        /\ status[run] = "running"
        /\ status' = [status EXCEPT ![run] = "failed"]
        /\ workerRun' = [workerRun EXCEPT ![worker] = None]
        /\ waitingKind' = [waitingKind EXCEPT ![run] = "none"]
        /\ dueTimers' = dueTimers \ {run}
        /\ needsEnqueue' = needsEnqueue \ {run}
        /\ callbackPending' = callbackPending \cup {run}
    /\ UNCHANGED << queue, seenSignals, nodeDone, callbackDelivered >>

SuspendTimer(worker) ==
    /\ workerRun[worker] # None
    /\ LET run == workerRun[worker]
       IN
        /\ status[run] = "running"
        /\ run \notin nodeDone
        /\ status' = [status EXCEPT ![run] = "waiting"]
        /\ workerRun' = [workerRun EXCEPT ![worker] = None]
        /\ waitingKind' = [waitingKind EXCEPT ![run] = "timer"]
        /\ dueTimers' = dueTimers \ {run}
        /\ needsEnqueue' = needsEnqueue \ {run}
    /\ UNCHANGED << queue, seenSignals, nodeDone, callbackPending, callbackDelivered >>

SuspendSignal(worker) ==
    /\ workerRun[worker] # None
    /\ LET run == workerRun[worker]
       IN
        /\ status[run] = "running"
        /\ run \notin nodeDone
        /\ status' = [status EXCEPT ![run] = "waiting"]
        /\ workerRun' = [workerRun EXCEPT ![worker] = None]
        /\ waitingKind' = [waitingKind EXCEPT ![run] = "signal"]
        /\ dueTimers' = dueTimers \ {run}
        /\ needsEnqueue' = needsEnqueue \ {run}
    /\ UNCHANGED << queue, seenSignals, nodeDone, callbackPending, callbackDelivered >>

TimerBecomesDue(run) ==
    /\ status[run] = "waiting"
    /\ waitingKind[run] = "timer"
    /\ run \notin dueTimers
    /\ dueTimers' = dueTimers \cup {run}
    /\ UNCHANGED
        << status,
           queue,
           workerRun,
           waitingKind,
           needsEnqueue,
           seenSignals,
           nodeDone,
           callbackPending,
           callbackDelivered >>

TimerResume(run) ==
    /\ status[run] = "waiting"
    /\ waitingKind[run] = "timer"
    /\ run \in dueTimers
    /\ status' = [status EXCEPT ![run] = "queued"]
    /\ waitingKind' = [waitingKind EXCEPT ![run] = "none"]
    /\ dueTimers' = dueTimers \ {run}
    /\ needsEnqueue' = needsEnqueue \cup {run}
    /\ nodeDone' = nodeDone \cup {run}
    /\ UNCHANGED << queue, workerRun, seenSignals, callbackPending, callbackDelivered >>

SignalArrives(run, signal) ==
    /\ <<run, signal>> \notin seenSignals
    /\ seenSignals' = seenSignals \cup {<<run, signal>>}
    /\ IF status[run] = "waiting" /\ waitingKind[run] = "signal"
       THEN
        /\ status' = [status EXCEPT ![run] = "queued"]
        /\ waitingKind' = [waitingKind EXCEPT ![run] = "none"]
        /\ dueTimers' = dueTimers \ {run}
        /\ needsEnqueue' = needsEnqueue \cup {run}
        /\ nodeDone' = nodeDone \cup {run}
       ELSE
        /\ UNCHANGED << status, waitingKind, dueTimers, needsEnqueue, nodeDone >>
    /\ UNCHANGED << queue, workerRun, callbackPending, callbackDelivered >>

Cancel(run) ==
    /\ status[run] \notin TerminalStatuses
    /\ status[run] # "absent"
    /\ status' = [status EXCEPT ![run] = "cancelled"]
    /\ workerRun' = ReleaseWorkerFor(run)
    /\ waitingKind' = [waitingKind EXCEPT ![run] = "none"]
    /\ dueTimers' = dueTimers \ {run}
    /\ needsEnqueue' = needsEnqueue \ {run}
    /\ callbackPending' = callbackPending \cup {run}
    /\ UNCHANGED << queue, seenSignals, nodeDone, callbackDelivered >>

Restart ==
    /\ ProcessingRuns # {}
    /\ status' = [r \in Runs |->
        IF status[r] = "running" THEN "queued" ELSE status[r]]
    /\ workerRun' = [w \in Workers |-> None]
    /\ needsEnqueue' = needsEnqueue \cup ProcessingRuns
    /\ UNCHANGED
        << queue,
           waitingKind,
           dueTimers,
           seenSignals,
           nodeDone,
           callbackPending,
           callbackDelivered >>

DeliverCallback(run) ==
    /\ run \in callbackPending
    /\ callbackPending' = callbackPending \ {run}
    /\ callbackDelivered' = callbackDelivered \cup {run}
    /\ UNCHANGED
        << status,
           queue,
           workerRun,
           waitingKind,
           dueTimers,
           needsEnqueue,
           seenSignals,
           nodeDone >>

Next ==
    \/ \E run \in Runs : Submit(run)
    \/ \E run \in Runs : EnqueuePending(run)
    \/ \E run \in Runs : DuplicateEnqueue(run)
    \/ \E worker \in Workers : Dequeue(worker)
    \/ \E worker \in Workers : CompleteRun(worker)
    \/ \E worker \in Workers : FailRun(worker)
    \/ \E worker \in Workers : SuspendTimer(worker)
    \/ \E worker \in Workers : SuspendSignal(worker)
    \/ \E run \in Runs : TimerBecomesDue(run)
    \/ \E run \in Runs : TimerResume(run)
    \/ \E run \in Runs, signal \in Signals : SignalArrives(run, signal)
    \/ \E run \in Runs : Cancel(run)
    \/ Restart
    \/ \E run \in Runs : DeliverCallback(run)

Init ==
    /\ status = [r \in Runs |-> "absent"]
    /\ queue = <<>>
    /\ workerRun = [w \in Workers |-> None]
    /\ waitingKind = [r \in Runs |-> "none"]
    /\ dueTimers = {}
    /\ needsEnqueue = {}
    /\ seenSignals = {}
    /\ nodeDone = {}
    /\ callbackPending = {}
    /\ callbackDelivered = {}

Spec == Init /\ [][Next]_vars

TerminalityProperty ==
    \A run \in Runs :
        /\ [](status[run] = "completed" => [](status[run] = "completed"))
        /\ [](status[run] = "failed" => [](status[run] = "failed"))
        /\ [](status[run] = "cancelled" => [](status[run] = "cancelled"))

NodeDoneStableProperty ==
    \A run \in Runs :
        [](run \in nodeDone => [](run \in nodeDone))

SeenSignalStableProperty ==
    \A pair \in SignalPairs :
        [](pair \in seenSignals => [](pair \in seenSignals))

CallbackDeliveredStableProperty ==
    \A run \in Runs :
        [](run \in callbackDelivered => [](run \in callbackDelivered))

=============================================================================
