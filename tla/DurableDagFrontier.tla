-------------------------- MODULE DurableDagFrontier --------------------------
EXTENDS FiniteSets, Naturals

(*
This model checks durable DAG frontier bookkeeping for a small resolution graph:

    fetch --success--> submit
    fetch --retry----> wait --back-edge--> fetch

The model focuses on Activated, Completed, Failed, Skipped, and Waiting state.
It abstracts executor internals and CEL into nondeterministic fetch outcomes.
The important safety property is that a completed node can only run again after
an explicit reactivation, while the terminal submit node is never duplicated.
*)

CONSTANTS
    Runs,
    Fetch,
    Wait,
    Submit,
    NoneNode,
    MaxBacktracks

ASSUME
    /\ Runs # {}
    /\ Cardinality({Fetch, Wait, Submit, NoneNode}) = 4
    /\ MaxBacktracks \in Nat

Nodes == {Fetch, Wait, Submit}
RunStatuses == {"active", "completed", "failed"}
TerminalStatuses == {"completed", "failed"}
CountRange == 0..(MaxBacktracks + 2)

VARIABLES
    runStatus,
    activated,
    completed,
    failed,
    skipped,
    waiting,
    running,
    edgeTraversals,
    executionCount,
    reactivationCount,
    waitSuspendCount,
    waitResumeCount

vars ==
    << runStatus,
       activated,
       completed,
       failed,
       skipped,
       waiting,
       running,
       edgeTraversals,
       executionCount,
       reactivationCount,
       waitSuspendCount,
       waitResumeCount >>

RunningSet(run) ==
    IF running[run] = NoneNode THEN {} ELSE {running[run]}

NodeInactive(run, node) ==
    /\ node \notin (completed[run] \cup failed[run] \cup skipped[run] \cup waiting[run])
    /\ running[run] # node

Ready(run, node) ==
    /\ runStatus[run] = "active"
    /\ running[run] = NoneNode
    /\ node \in activated[run]
    /\ NodeInactive(run, node)
    /\ IF node = Fetch THEN TRUE ELSE Fetch \in completed[run]

NoReady(run) ==
    \A node \in Nodes : ~Ready(run, node)

ReactivateCount(rc, run, node, wasCompleted) ==
    IF wasCompleted
    THEN [rc EXCEPT ![run][node] = @ + 1]
    ELSE rc

TypeOK ==
    /\ runStatus \in [Runs -> RunStatuses]
    /\ activated \in [Runs -> SUBSET Nodes]
    /\ completed \in [Runs -> SUBSET Nodes]
    /\ failed \in [Runs -> SUBSET Nodes]
    /\ skipped \in [Runs -> SUBSET Nodes]
    /\ waiting \in [Runs -> SUBSET Nodes]
    /\ running \in [Runs -> Nodes \cup {NoneNode}]
    /\ edgeTraversals \in [Runs -> 0..MaxBacktracks]
    /\ executionCount \in [Runs -> [Nodes -> CountRange]]
    /\ reactivationCount \in [Runs -> [Nodes -> CountRange]]
    /\ waitSuspendCount \in [Runs -> CountRange]
    /\ waitResumeCount \in [Runs -> CountRange]

FrontierSetsDisjoint ==
    \A run \in Runs :
        /\ completed[run] \cap failed[run] = {}
        /\ completed[run] \cap skipped[run] = {}
        /\ completed[run] \cap waiting[run] = {}
        /\ failed[run] \cap skipped[run] = {}
        /\ failed[run] \cap waiting[run] = {}
        /\ skipped[run] \cap waiting[run] = {}

RunningNodeClean ==
    \A run \in Runs :
        running[run] # NoneNode =>
            /\ runStatus[run] = "active"
            /\ running[run] \in activated[run]
            /\ running[run] \notin (completed[run] \cup failed[run] \cup skipped[run] \cup waiting[run])

TouchedNodesActivated ==
    \A run \in Runs :
        completed[run] \cup failed[run] \cup skipped[run] \cup waiting[run] \cup RunningSet(run)
            \subseteq activated[run]

TerminalStateClean ==
    \A run \in Runs :
        runStatus[run] \in TerminalStatuses =>
            /\ running[run] = NoneNode
            /\ waiting[run] = {}

FetchAlwaysActivated ==
    \A run \in Runs : Fetch \in activated[run]

WaitingAccounting ==
    \A run \in Runs :
        /\ waitResumeCount[run] <= waitSuspendCount[run]
        /\ waitSuspendCount[run] <= waitResumeCount[run] + 1
        /\ (Wait \in waiting[run] => waitSuspendCount[run] = waitResumeCount[run] + 1)
        /\ (Wait \notin waiting[run] => waitSuspendCount[run] = waitResumeCount[run])
        /\ executionCount[run][Wait] = waitResumeCount[run]

NoRerunWithoutReactivation ==
    \A run \in Runs, node \in Nodes :
        executionCount[run][node] <= reactivationCount[run][node] + 1

BackEdgeReactivationAccounting ==
    \A run \in Runs :
        edgeTraversals[run] = reactivationCount[run][Fetch]

SubmitNeverReactivated ==
    \A run \in Runs : reactivationCount[run][Submit] = 0

SubmitExecutesAtMostOnce ==
    \A run \in Runs : executionCount[run][Submit] <= 1

SubmitOnlyAfterFetchCompleted ==
    \A run \in Runs :
        Submit \in completed[run] => Fetch \in completed[run]

SkippedNodesNeverExecuted ==
    \A run \in Runs, node \in Nodes :
        node \in skipped[run] => executionCount[run][node] = 0

FrontierSafety ==
    /\ FrontierSetsDisjoint
    /\ RunningNodeClean
    /\ TouchedNodesActivated
    /\ TerminalStateClean
    /\ FetchAlwaysActivated
    /\ WaitingAccounting
    /\ NoRerunWithoutReactivation
    /\ BackEdgeReactivationAccounting
    /\ SubmitNeverReactivated
    /\ SubmitExecutesAtMostOnce
    /\ SubmitOnlyAfterFetchCompleted
    /\ SkippedNodesNeverExecuted

StartReady(run, node) ==
    /\ Ready(run, node)
    /\ running' = [running EXCEPT ![run] = node]
    /\ UNCHANGED
        << runStatus,
           activated,
           completed,
           failed,
           skipped,
           waiting,
           edgeTraversals,
           executionCount,
           reactivationCount,
           waitSuspendCount,
           waitResumeCount >>

CompleteFetchSuccess(run) ==
    /\ running[run] = Fetch
    /\ runStatus[run] = "active"
    /\ running' = [running EXCEPT ![run] = NoneNode]
    /\ completed' = [completed EXCEPT ![run] = completed[run] \cup {Fetch}]
    /\ activated' = [activated EXCEPT ![run] = activated[run] \cup {Submit}]
    /\ executionCount' = [executionCount EXCEPT ![run][Fetch] = @ + 1]
    /\ UNCHANGED
        << runStatus,
           failed,
           skipped,
           waiting,
           edgeTraversals,
           reactivationCount,
           waitSuspendCount,
           waitResumeCount >>

CompleteFetchRetry(run) ==
    /\ running[run] = Fetch
    /\ runStatus[run] = "active"
    /\ edgeTraversals[run] < MaxBacktracks
    /\ running' = [running EXCEPT ![run] = NoneNode]
    /\ LET waitWasCompleted == Wait \in completed[run]
       IN
        /\ completed' = [completed EXCEPT ![run] =
            (completed[run] \cup {Fetch}) \ (IF waitWasCompleted THEN {Wait} ELSE {})]
        /\ reactivationCount' = ReactivateCount(reactivationCount, run, Wait, waitWasCompleted)
    /\ activated' = [activated EXCEPT ![run] = activated[run] \cup {Wait}]
    /\ executionCount' = [executionCount EXCEPT ![run][Fetch] = @ + 1]
    /\ UNCHANGED
        << runStatus,
           failed,
           skipped,
           waiting,
           edgeTraversals,
           waitSuspendCount,
           waitResumeCount >>

CompleteFetchFailure(run) ==
    /\ running[run] = Fetch
    /\ runStatus[run] = "active"
    /\ running' = [running EXCEPT ![run] = NoneNode]
    /\ runStatus' = [runStatus EXCEPT ![run] = "failed"]
    /\ failed' = [failed EXCEPT ![run] = failed[run] \cup {Fetch}]
    /\ skipped' = [skipped EXCEPT ![run] =
        skipped[run] \cup ((activated[run] \ {Fetch}) \ (completed[run] \cup failed[run] \cup waiting[run]))]
    /\ executionCount' = [executionCount EXCEPT ![run][Fetch] = @ + 1]
    /\ waiting' = [waiting EXCEPT ![run] = {}]
    /\ UNCHANGED
        << activated,
           completed,
           edgeTraversals,
           reactivationCount,
           waitSuspendCount,
           waitResumeCount >>

SuspendWait(run) ==
    /\ running[run] = Wait
    /\ runStatus[run] = "active"
    /\ running' = [running EXCEPT ![run] = NoneNode]
    /\ waiting' = [waiting EXCEPT ![run] = waiting[run] \cup {Wait}]
    /\ waitSuspendCount' = [waitSuspendCount EXCEPT ![run] = @ + 1]
    /\ UNCHANGED
        << runStatus,
           activated,
           completed,
           failed,
           skipped,
           edgeTraversals,
           executionCount,
           reactivationCount,
           waitResumeCount >>

ResumeWait(run) ==
    /\ runStatus[run] = "active"
    /\ running[run] = NoneNode
    /\ Wait \in waiting[run]
    /\ waiting' = [waiting EXCEPT ![run] = waiting[run] \ {Wait}]
    /\ executionCount' = [executionCount EXCEPT ![run][Wait] = @ + 1]
    /\ waitResumeCount' = [waitResumeCount EXCEPT ![run] = @ + 1]
    /\ IF edgeTraversals[run] < MaxBacktracks
       THEN
        /\ LET fetchWasCompleted == Fetch \in completed[run]
           IN
            /\ completed' = [completed EXCEPT ![run] =
                (completed[run] \cup {Wait}) \ (IF fetchWasCompleted THEN {Fetch} ELSE {})]
            /\ reactivationCount' = ReactivateCount(reactivationCount, run, Fetch, fetchWasCompleted)
        /\ activated' = [activated EXCEPT ![run] = activated[run] \cup {Fetch}]
        /\ edgeTraversals' = [edgeTraversals EXCEPT ![run] = @ + 1]
       ELSE
        /\ completed' = [completed EXCEPT ![run] = completed[run] \cup {Wait}]
        /\ UNCHANGED << activated, edgeTraversals, reactivationCount >>
    /\ UNCHANGED
        << runStatus,
           failed,
           skipped,
           running,
           waitSuspendCount >>

CompleteSubmit(run) ==
    /\ running[run] = Submit
    /\ runStatus[run] = "active"
    /\ running' = [running EXCEPT ![run] = NoneNode]
    /\ runStatus' = [runStatus EXCEPT ![run] = "completed"]
    /\ completed' = [completed EXCEPT ![run] = completed[run] \cup {Submit}]
    /\ executionCount' = [executionCount EXCEPT ![run][Submit] = @ + 1]
    /\ UNCHANGED
        << activated,
           failed,
           skipped,
           waiting,
           edgeTraversals,
           reactivationCount,
           waitSuspendCount,
           waitResumeCount >>

NoReadyFails(run) ==
    /\ runStatus[run] = "active"
    /\ running[run] = NoneNode
    /\ waiting[run] = {}
    /\ Submit \notin completed[run]
    /\ NoReady(run)
    /\ runStatus' = [runStatus EXCEPT ![run] = "failed"]
    /\ skipped' = [skipped EXCEPT ![run] =
        skipped[run] \cup (activated[run] \ (completed[run] \cup failed[run]))]
    /\ UNCHANGED
        << activated,
           completed,
           failed,
           waiting,
           running,
           edgeTraversals,
           executionCount,
           reactivationCount,
           waitSuspendCount,
           waitResumeCount >>

RestartRun(run) ==
    /\ running[run] # NoneNode
    /\ running' = [running EXCEPT ![run] = NoneNode]
    /\ UNCHANGED
        << runStatus,
           activated,
           completed,
           failed,
           skipped,
           waiting,
           edgeTraversals,
           executionCount,
           reactivationCount,
           waitSuspendCount,
           waitResumeCount >>

Next ==
    \/ \E run \in Runs, node \in Nodes : StartReady(run, node)
    \/ \E run \in Runs : CompleteFetchSuccess(run)
    \/ \E run \in Runs : CompleteFetchRetry(run)
    \/ \E run \in Runs : CompleteFetchFailure(run)
    \/ \E run \in Runs : SuspendWait(run)
    \/ \E run \in Runs : ResumeWait(run)
    \/ \E run \in Runs : CompleteSubmit(run)
    \/ \E run \in Runs : NoReadyFails(run)
    \/ \E run \in Runs : RestartRun(run)

Init ==
    /\ runStatus = [run \in Runs |-> "active"]
    /\ activated = [run \in Runs |-> {Fetch}]
    /\ completed = [run \in Runs |-> {}]
    /\ failed = [run \in Runs |-> {}]
    /\ skipped = [run \in Runs |-> {}]
    /\ waiting = [run \in Runs |-> {}]
    /\ running = [run \in Runs |-> NoneNode]
    /\ edgeTraversals = [run \in Runs |-> 0]
    /\ executionCount = [run \in Runs |-> [node \in Nodes |-> 0]]
    /\ reactivationCount = [run \in Runs |-> [node \in Nodes |-> 0]]
    /\ waitSuspendCount = [run \in Runs |-> 0]
    /\ waitResumeCount = [run \in Runs |-> 0]

Spec == Init /\ [][Next]_vars

TerminalityProperty ==
    \A run \in Runs :
        /\ [](runStatus[run] = "completed" => [](runStatus[run] = "completed"))
        /\ [](runStatus[run] = "failed" => [](runStatus[run] = "failed"))

=============================================================================
