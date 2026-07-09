# Subtask A — Task Model + Queue: Tickets (3 programmers)

Source: `plan.md` §0.1, §0.2, "Subtask A" section. Scope: `mobilerun/orchestration/models.py`, `mobilerun/orchestration/queue.py`, and their tests. **No dependency on Subtask B or C** — these tickets can start immediately.

Branch convention: `feat/orchestration-a1`, `feat/orchestration-a2`, `feat/orchestration-a3`.
Commit convention: `feat(orchestration): <summary>`.

---

## Ticket A1 — Shared data model (`models.py`)

**Status: ✅ Done** — `mobilerun/orchestration/models.py` + `tests/test_task_models.py` implemented and verified (`pytest tests/test_task_models.py -v` → 9 passed; `ruff check` clean; full `pytest tests/` shows no new failures vs. baseline — see notes for A2/A3 below).

**Owner:** Member 1
**Blocks:** A2, A3 (they import from this file — must land first, even just as a stub PR, before A2/A3 can merge for real)
**Depends on:** nothing

### Scope
Create `mobilerun/orchestration/models.py` with:

- `TaskStatus(str, Enum)` — `WAITING`, `RUNNING`, `COMPLETED`, `FAILED`, `CANCELLED`
- `TaskRequest` dataclass — `goal`, `id` (uuid4 hex default), `created_at` (datetime.now default), `scheduled_at`, `repeat_every`, `priority`, `device_serial`, `llm_provider`, `llm_model`, `config_path`, `timeout=1000`, `metadata` (dict default)
- `TaskResult` dataclass — `task_id`, `success`, `reason`, `steps`, `structured_output`, `error`, `started_at`, `finished_at`
- `TaskRecord` dataclass — `request`, `status` (default `WAITING`), `result`
- `InvalidTransitionError(Exception)` — raised by the queue (A2) on illegal status transitions; **defined here**, not in `queue.py`, since both `queue.py` and CLI/tests need to catch it
- `to_dict()` / `from_dict()` helpers for `TaskRequest` and `TaskResult` (JSON-serializable — needed later for `--file tasks.yaml` in C3 and for any future persistence swap)

### Exact field types per plan.md §0.1
```python
scheduled_at: datetime | None = None
repeat_every: timedelta | None = None
priority: int = 0
```

### Acceptance criteria
- All dataclasses importable from `mobilerun.orchestration.models`
- `TaskRequest()` with only `goal` set works and produces a unique `id` each time
- `to_dict()` → `from_dict()` round-trips exactly (datetimes and timedeltas serialize/deserialize losslessly — use ISO 8601 for datetimes, total_seconds() for timedeltas)
- `TaskStatus` values are the exact lowercase strings in the table above (CLI output and tests will compare against these literal strings)

### Tests owned: `tests/test_task_models.py`
- Defaults (goal-only construction)
- ID uniqueness across instances
- Serialization round-trip (`to_dict`/`from_dict`)
- Status enum values match spec exactly

### ⚠️ Watch out for
- **Mutable default pitfall:** `metadata: dict = field(default_factory=dict)` — not `= {}`. Same for `id` and `created_at` — must use `field(default_factory=...)`, not a bare default, or every task will share the same dict/timestamp.
- `datetime.now()` vs `datetime.now(timezone.utc)` — pick one and document it in the module docstring. Mixing naive/aware datetimes later breaks `scheduled_at <= now` comparisons in A2 and C1. **Recommend UTC-aware throughout** — flag this decision to the team before A2/C1 start, since it's expensive to change later.

### Notes for A2/A3 (from implementing A1)
- **Decision locked:** `created_at`/`scheduled_at`/`started_at`/`finished_at` are all `datetime.now(timezone.utc)` (aware). **A2's `dequeue()` readiness check (`scheduled_at <= now`) and C1's scheduler must call `datetime.now(timezone.utc)` too** — comparing an aware `TaskRequest.scheduled_at` against a naive `datetime.now()` raises `TypeError` at runtime, not a silent bug, so it'll surface immediately in tests, but worth getting right from the start.
- `to_dict()`/`from_dict()` on `TaskRequest`/`TaskResult` mirror the existing `MobileConfig.to_dict/from_dict` pattern in `mobilerun/config_manager/config_manager.py:270-299` (`asdict()` + manual patch of datetime/timedelta fields). `TaskRecord` intentionally has **no** `to_dict`/`from_dict` yet — not required by any ticket. If C3's `--file tasks.yaml` (or persistence) ends up needing to serialize a whole `TaskRecord` (request + status + result), add it then; it composes trivially from the two existing methods.
- `InvalidTransitionError` is a plain `Exception` with no structured fields (no `task_id`/`from_status`/`to_status` attributes) — A2 should pass a descriptive message string when raising it (e.g. `f"cannot mark {task_id} completed from status {current}"'`); nothing stops adding structured attributes later if the CLI (C3) wants to print something more specific, but that's not blocking.
- **Environment note:** this repo's dev tools (`pytest`, `ruff`) are not installed by default — run `pip install -e ".[dev]"` once per environment before testing. After doing that, running the full suite (`pytest tests/`) shows **12 pre-existing failures unrelated to orchestration**: `ModuleNotFoundError: llama_index.llms.anthropic` (optional dep not installed), one ANSI-color-code assertion in `test_cloud_cli.py`, and one `UnicodeDecodeError` (cp1252) reading a UTF-8 source file with emoji in `test_visual_remote_connection.py`. These are Windows/optional-dependency environment gaps, not regressions from this work — don't chase them down as part of Subtask A.

---

## Ticket A2 — Queue core (`queue.py`)

**Owner:** Member 2
**Depends on:** A1 (needs real models, not stubs, to merge — can dev against stub in parallel)
**Blocks:** A3's final merge, all of Subtask B, C1

### Scope
Implement the core of `TaskQueue` in `mobilerun/orchestration/queue.py`:

```python
class TaskQueue:
    def submit(self, request: TaskRequest) -> str
    async def dequeue(self) -> TaskRequest
    def mark_running(self, task_id: str) -> None
    def mark_completed(self, task_id: str, result: TaskResult) -> None
    def mark_failed(self, task_id: str, result: TaskResult) -> None
    def wake(self) -> None
```

- **Storage:** in-memory dict `{task_id: TaskRecord}` — keep the storage access behind a small private method (e.g. `_store`) so a future DB-backed swap doesn't require touching `dequeue`/ordering logic (per plan.md's "swappable" requirement)
- **`dequeue()`:** blocks on `asyncio.Condition` until a task is READY (`scheduled_at is None or scheduled_at <= now`). Ordering: **priority desc, then `created_at` asc (FIFO)** among ready tasks.
- **`wake()`:** notifies the condition so waiters re-check readiness (called externally by the scheduler when a timer fires, and internally by `submit`)
- **Transition validation:** legal transitions only —
  `WAITING → RUNNING → {COMPLETED, FAILED}`, and `WAITING → CANCELLED` (cancel is A3's job, but the *validation* rule belongs here since A3 builds on this class). Anything else raises `InvalidTransitionError`.

### Acceptance criteria
- `dequeue()` never busy-polls — must use `asyncio.Condition.wait()`, not a sleep loop
- Two tasks with the same priority dequeue in submission order
- A task with `scheduled_at` in the future is *not* returned by `dequeue()` even if it's the only task in the queue (caller blocks until `wake()` is called after the time passes, or until `scheduled_at` — see note below)
- `mark_completed`/`mark_failed` on a task not in `RUNNING` raises `InvalidTransitionError`

### Tests owned: `tests/test_task_queue.py`
- FIFO & priority order
- `dequeue` blocks then returns after `submit`/`wake`
- `scheduled_at` gating (future task not returned early)
- Illegal transitions raise

### ⚠️ Watch out for — this is the trickiest ticket in Subtask A
- **`dequeue` timeout semantics are underspecified in plan.md.** `asyncio.Condition.wait()` blocks *forever* until notified — it does not wake up on its own when a future `scheduled_at` elapses. Two options:
  1. `dequeue()` uses `wait_for(timeout=...)` computed from the earliest `scheduled_at` in the queue, so it self-wakes without needing an external nudge.
  2. `dequeue()` relies entirely on the scheduler (C1) calling `wake()` at the right time, and never self-computes a timeout.
  **Pick one and confirm with C1's owner before writing tests** — plan.md's C1 description ("arms `asyncio` timers ... calls `queue.wake()`") suggests option 2, but that means `TaskQueue` alone is not correct/complete without a `Scheduler` running — worth calling out explicitly in the module docstring so nobody "fixes" this later as a bug.
- **Race between `submit` and a waiting `dequeue`:** acquire the condition's lock before mutating the store and notify while holding it, or you'll get lost wakeups under `pytest-asyncio` (a `wake()` that fires with no active `wait()` yet is a no-op, and dequeue can permanently block if the check-then-wait isn't atomic with the lock).
- Priority ordering with FIFO tiebreak means you need a stable sort or a heap keyed on `(-priority, created_at)` — a naive `min()`/`sorted()` scan each time is fine at MVP scale (single agent, small queue) but say so in a comment if you take that shortcut, so it's not "optimized" prematurely later.

---

## Ticket A3 — Queue query/cancel/listeners API

**Owner:** Member 3
**Depends on:** A2 (can start against a stub `TaskQueue` class with the right method signatures, per plan.md's build order: "A2 ∥ A3 against stubs")
**Blocks:** C2 (recurrence, listens for completion), C3 (live CLI status table)

### Scope
Add to the same `TaskQueue` class (coordinate merge order with A2 — **do not fork the class into two files**):

```python
def get(self, task_id: str) -> TaskRecord | None
def list(self, status: TaskStatus | None = None) -> list[TaskRecord]
def cancel(self, task_id: str) -> bool
def add_listener(self, cb: Callable[[TaskRecord], None]) -> None
```

- `cancel`: only cancels `WAITING` tasks (a task already `RUNNING` cannot be cancelled mid-flight in the MVP — there's no cooperative cancellation hook into `MobileAgent`). Returns `True` iff it actually cancelled something.
- `add_listener`: callback fires on **every** status change (`mark_running`, `mark_completed`, `mark_failed`, `cancel`) — exactly once per transition. Listener exceptions must not break the queue (catch and log, don't propagate).

### Acceptance criteria
- `list(status=TaskStatus.WAITING)` filters correctly; `list()` with no arg returns everything
- `cancel()` on a `RUNNING` or already-terminal task returns `False` and does not raise
- Listener registered before a transition receives exactly one call per transition, with the updated `TaskRecord`
- A listener that raises does not prevent the queue from completing the transition or notify other listeners incorrectly (decide and document: do other listeners still get called after one throws? Recommend yes — isolate each callback in its own try/except)

### Tests owned: `tests/test_task_queue_api.py`
- List filtering
- Cancel only-`WAITING` semantics
- Listener fired once per transition

### ⚠️ Watch out for
- **Merge conflict risk with A2 is the biggest coordination issue in Subtask A** — both of you are editing the same `TaskQueue` class in the same file. Plan.md explicitly calls this out ("A2 skeleton (stub signatures suffice to start) → A3 merges onto A2's real class"). Agree on method order in the file and merge frequently in small diffs rather than both working for days and reconciling a huge diff at the end.
- Listener callback signature is sync (`Callable[[TaskRecord], None]`) even though the queue itself is partly async — don't accidentally make `add_listener`'s callback awaitable; that would require the queue to `await` inside a sync `mark_*` method, which isn't in the contract in plan.md and would ripple into A2's transition methods.
- `cancel()` needs to interact with A2's `asyncio.Condition` too: if a cancelled task was the only thing keeping a `dequeue()` waiter from timing out/blocking on it, no special handling is needed since a cancelled task simply won't be returned — but double check `dequeue()`'s readiness scan skips `CANCELLED` records rather than erroring on them.

---

## Cross-cutting issues for all three (surface to the team before coding)

1. **`InvalidTransitionError` location** — plan.md doesn't say where it's defined, only that it's "defined in `models.py`" per the footnote under §0.2. A1 must include it; A2/A3 must import it from there, not redefine it.
2. **Timezone convention** (naive vs. UTC-aware `datetime`) — pick once in A1, document, and make sure A2's `scheduled_at <= now` comparison and C1's scheduler use the same convention. Mixing them raises `TypeError: can't compare offset-naive and offset-aware datetimes` at runtime, and it's a common cross-team integration bug.
3. **The `dequeue()` self-wake vs. scheduler-driven-wake decision (A2 ⚠️ above)** is the single highest-risk open design question in this subtask — it changes what "correct" means for `test_task_queue.py`'s scheduling-gate tests, and it's a contract Subtask C's scheduler depends on. Resolve it in the Step 0 sign-off, not mid-implementation.
4. **A3 depends on A2's exact internal storage shape** (dict keyed by `task_id` is assumed above) — if A2 changes the internal representation, `get`/`list` need to change too. Keep the internal store as a single private attribute so this is a one-line fix, not a rewrite.
5. All three chunks should target ≥90% coverage on their own file before opening a PR — this queue is the foundation every other subtask blocks on; a bug here (e.g. a lost wakeup, wrong ordering) surfaces as a flaky integration test two weeks later and is much harder to root-cause at that point.
