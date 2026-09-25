"""A small dependency-aware DAG runner in plain Python.

Each task is a zero-argument function registered with the names of the tasks
it depends on. Tasks run one at a time in topological order. If a task raises,
every task downstream of it is skipped, while tasks on independent branches
still run.

Example:
    dag = DAG("freight")

    @dag.task()
    def extract(): ...

    @dag.task(upstream=["extract"])
    def transform(): ...

    result = dag.run()
    result.raise_for_failures()
"""

import logging
import time
import traceback
from dataclasses import dataclass, field
from enum import Enum

log = logging.getLogger(__name__)


class DAGError(Exception):
    """Base class for DAG definition errors."""


class CycleError(DAGError):
    """The task graph contains a cycle."""

    def __init__(self, cycle):
        self.cycle = cycle
        super().__init__("Cycle detected in DAG: " + " -> ".join(cycle))


class MissingUpstreamError(DAGError):
    """A task depends on a task that was never registered."""


class DuplicateTaskError(DAGError):
    """Two tasks were registered under the same name."""


class TaskFailedError(Exception):
    """Raised by DAGRunResult.raise_for_failures() when any task failed."""


class TaskState(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"  # an upstream task failed or was skipped


@dataclass
class Task:
    name: str
    fn: object
    upstream: tuple


@dataclass
class TaskResult:
    name: str
    state: TaskState
    output: object = None
    error: BaseException = None
    duration_s: float = 0.0
    skipped_because: str = None  # the upstream task that caused the skip


@dataclass
class DAGRunResult:
    order: list
    results: dict = field(default_factory=dict)

    @property
    def succeeded(self):
        return all(r.state is TaskState.SUCCESS for r in self.results.values())

    def names_in(self, state):
        return [n for n in self.order if self.results[n].state is state]

    def raise_for_failures(self):
        failed = self.names_in(TaskState.FAILED)
        if failed:
            skipped = self.names_in(TaskState.SKIPPED)
            raise TaskFailedError(
                f"Failed tasks: {', '.join(failed)}"
                + (f"; skipped downstream: {', '.join(skipped)}" if skipped else "")
            ) from self.results[failed[0]].error


class DAG:
    def __init__(self, name="dag"):
        self.name = name
        self.tasks = {}

    def add_task(self, name, fn, upstream=()):
        if name in self.tasks:
            raise DuplicateTaskError(f"Task {name!r} is already registered")
        if isinstance(upstream, str):
            upstream = (upstream,)
        self.tasks[name] = Task(name, fn, tuple(upstream))
        return fn

    def task(self, name=None, upstream=()):
        """Decorator form of add_task. The name defaults to the function's name."""
        def register(fn):
            return self.add_task(name or fn.__name__, fn, upstream)
        return register

    def validate(self):
        for t in self.tasks.values():
            missing = [u for u in t.upstream if u not in self.tasks]
            if missing:
                raise MissingUpstreamError(
                    f"Task {t.name!r} depends on unknown task(s): {', '.join(missing)}")
            if t.name in t.upstream:
                raise CycleError([t.name, t.name])

    def topological_order(self):
        """Kahn's algorithm. Ties keep registration order, so the order is deterministic."""
        self.validate()
        indegree = {n: len(set(t.upstream)) for n, t in self.tasks.items()}
        downstream = {n: [] for n in self.tasks}
        for t in self.tasks.values():
            for u in set(t.upstream):
                downstream[u].append(t.name)

        ready = [n for n in self.tasks if indegree[n] == 0]
        order = []
        while ready:
            n = ready.pop(0)
            order.append(n)
            for d in downstream[n]:
                indegree[d] -= 1
                if indegree[d] == 0:
                    ready.append(d)

        if len(order) != len(self.tasks):
            raise CycleError(self._find_cycle({n for n in self.tasks if indegree[n] > 0}))
        return order

    def _find_cycle(self, candidates):
        """Return one concrete cycle, e.g. ['a', 'b', 'c', 'a'], for the error message."""
        visiting, done = [], set()

        def dfs(n):
            visiting.append(n)
            for u in self.tasks[n].upstream:
                if u in visiting:
                    # Found a back edge; the list runs downstream -> upstream, so flip it.
                    return list(reversed(visiting[visiting.index(u):] + [u]))
                if u in candidates and u not in done:
                    cycle = dfs(u)
                    if cycle:
                        return cycle
            visiting.pop()
            done.add(n)
            return None

        for n in self.tasks:
            if n in candidates and n not in done:
                cycle = dfs(n)
                if cycle:
                    return cycle
        return sorted(candidates)  # unreachable for a real cycle; kept as a fallback

    def run(self):
        """Run every task in dependency order and return a DAGRunResult.

        Definition errors (cycles, missing upstreams) raise before any task runs.
        Task exceptions are caught and recorded, and all downstream tasks are skipped.
        """
        order = self.topological_order()
        run = DAGRunResult(order=order)
        log.info("DAG %r: running %d tasks: %s", self.name, len(order), " -> ".join(order))

        for name in order:
            task = self.tasks[name]
            blocker = next((u for u in task.upstream
                            if run.results[u].state is not TaskState.SUCCESS), None)
            if blocker:
                run.results[name] = TaskResult(name, TaskState.SKIPPED, skipped_because=blocker)
                log.warning("SKIPPED %s (upstream %r %s)", name, blocker,
                            run.results[blocker].state.value)
                continue

            start = time.perf_counter()
            try:
                output = task.fn()
            except Exception as exc:
                run.results[name] = TaskResult(name, TaskState.FAILED, error=exc,
                                               duration_s=time.perf_counter() - start)
                log.error("FAILED %s: %r\n%s", name, exc, traceback.format_exc())
            else:
                run.results[name] = TaskResult(name, TaskState.SUCCESS, output=output,
                                               duration_s=time.perf_counter() - start)
                log.info("SUCCESS %s (%.2fs)", name, run.results[name].duration_s)

        log.info("DAG %r finished: %d succeeded, %d failed, %d skipped", self.name,
                 len(run.names_in(TaskState.SUCCESS)), len(run.names_in(TaskState.FAILED)),
                 len(run.names_in(TaskState.SKIPPED)))
        return run
