# Issue 111 Design

## Title

Async processes and jobs: OGC API compliance, progress reporting, retry, and resume

## Purpose

This note replaces the older `#89` framing and aligns implementation planning with the current repository state after `CLIM-689`.

The most important clarification is:

- `CLIM-689` already delivered the native process surface
- Issue `#111` should now focus on the native job and async execution framework built on top of that surface

This means `#111` is no longer a mixed “process registry + jobs” ticket. The process registry and native `/processes` discovery/execution baseline already exist.

## Current Status

`CLIM-703` has now implemented the first native job-framework slice described here:

- persisted `jobs.json` store
- `/jobs` list/detail/cancel endpoints
- async process execution via `Prefer: respond-async`
- synchronous fallback when `Prefer` is absent
- cooperative cancellation
- retry with `retryAfter` delay semantics (`60s`, `120s`, `240s`)
- startup recovery for `accepted`, `running`, and `retrying` jobs
- executor abstraction with default in-process thread-backed implementation
- `resample` wired through the framework as the first concrete process

What remains after `CLIM-703` is downstream adoption and refinement rather than first-framework construction:

- ingestion migration onto the native job framework
- sync migration onto the native job framework
- richer executor backends if needed
- true resumable build behavior through `#64`

## One-Sentence Model

A **process** defines what operation can be run.

A **job** is one concrete execution instance of that process or of another long-running operation.

An **executor** defines how that concrete job is run.

## What 689 Already Delivered

`CLIM-689` established:

- native `GET /processes`
- native `GET /processes/{id}`
- native `POST /processes/{id}/execution`
- YAML-backed process registry
- OGC-oriented process response shapes
- `resample` as the first built-in published process

That work should be treated as an already-satisfied prerequisite, not as open scope for `#111`.

## What 111 Should Mean Now

Issue `#111` should implement the **job framework layer**:

- async submission
- persisted job state
- polling
- progress reporting
- cancellation semantics
- retry semantics
- restart recovery / resumability hooks
- pluggable execution backends

It should not reopen:

- native FastAPI vs pygeoapi ownership of `/processes`
- process registry design
- process discovery shape

## Decision Summary

Build one **native FastAPI job layer** shared by:

- process execution
- later ingestion, via its own migration ticket
- later sync, via its own migration ticket

This job layer should:

- persist state to disk
- support async submission using `Prefer: respond-async`
- keep synchronous fallback initially
- expose generic `/jobs` endpoints
- define framework-level cancellation, retry, and restart-recovery behavior
- provide an executor abstraction so specific job types can choose how work is run
- evolve toward OGC API Processes job semantics where relevant

## Relationship to Other Work

### `CLIM-689`

Already done and foundational.

`#111` extends that work rather than replacing or closing over it conceptually.

### Issue `#40`

This is enabling infrastructure for extensibility. Custom processes become much more operationally credible once they can run as jobs with progress and durable state.

### `CLIM-681`

Normals/anomalies are exactly the kind of derived workflows that benefit from a job model. That is why `#111` should come before or alongside serious `681` implementation.

### `#64`

`#64` is the real prerequisite for true resumable Zarr build behavior. `#111` can lay the job framework and resume hooks, but it should not claim that full build resume is solved without incremental append/checkpoint support.

### `#107`

`#107` (`_frequency_to_iso_resolution` loses the multiplier) is not a hard prerequisite for the job framework itself, but it is a recommended precondition if `resample` is the first built-in process used to validate the async path end to end. Otherwise `#111` would be proving the framework through a process that still has known metadata bugs.

### Separate ingestion/sync migration tickets

Issue `#111` should build the shared job framework once.

Separate tickets should:

- implement ingestion as a first-class process on the native job framework
- implement sync as a first-class process on the native job framework

That keeps the framework ticket focused while still making it responsible for the cross-cutting semantics those later migrations depend on.

The shared framework part of that work is now in place; these migration tickets remain the next major consumers.

## Current Repo Reality

This section captures the pre-migration baseline that motivated the design. Process execution is no longer purely synchronous in the current branch state because `CLIM-703` added async job execution for native processes.

### Process execution today

`POST /processes/{id}/execution` supports both:

- synchronous execution by default
- async execution when `Prefer: respond-async` is sent

- route: `climate_api/processing/routes.py`
- built-in example: `climate_api.processing.services.execute_resample`

### Ingestion today

`POST /ingestions` is synchronous:

- route: `climate_api/ingestions/routes.py`
- service: `climate_api/ingestions/services.py:create_artifact`

### Persistence today

- artifact records exist in `records.json`
- file locking already exists for artifact mutation
- there is no persisted job store yet

## Scope Recommendation

Issue `#111` should build the framework once, even if only `resample` is wired through it in the first implementation.

That means:

- do implement the shared job domain and runtime semantics in `#111`
- do not migrate ingestion or sync into first-class processes inside `#111`
- use `resample` as the first concrete built-in process that exercises the framework

Recommended phases within and around this work:

- Phase 1: persisted job store, `/jobs` endpoints, and async process execution
- Phase 2: cooperative cancellation and retry semantics in the framework
- Phase 3: restart recovery and resumability hooks

Downstream tickets enabled by this work:

- ingestion migration onto the framework
- sync migration onto the framework

## Recommended MVP Boundary

The first implementation slice should still be deliberate, but the framework design should not paint us into a corner.

### In scope

- `jobs.json` store under configured `data_dir`
- native job schemas and mutation helpers
- `GET /jobs`
- `GET /jobs/{job_id}`
- explicit deferral of `GET /jobs/{job_id}/results` for now, since `result` remains embedded in the job record during the first slice
- `POST /processes/{id}/execution` supports `Prefer: respond-async`
- sync fallback when `Prefer` is absent
- default in-process executor using `ThreadPoolExecutor`
- executor abstraction that allows later subprocess/custom/delegated runners
- persisted status transitions:
  - `accepted`
  - `running`
  - `retrying`
  - `successful`
  - `failed`
  - `cancelled`
- cooperative cancellation contract
- retry metadata and attempt tracking
- startup recovery policy for interrupted jobs

### Out of scope for the first slice

- ingestion-as-process migration
- `/sync` migration
- full OGC Parts 2/3 behavior
- sophisticated distributed workers or queue backends
- true resumable zarr build without `#64`

## API Shape

### Process execution

`POST /processes/{id}/execution`

- without `Prefer: respond-async`
  - keep current synchronous behavior
- with `Prefer: respond-async`
  - create job
  - return `202 Accepted`
  - return `Location: /jobs/{job_id}`

### Job polling

`GET /jobs/{job_id}`

Suggested response model:

```python
class JobRecord(BaseModel):
    job_id: str = Field(serialization_alias="jobID")
    process_id: str | None = Field(None, serialization_alias="processID")
    type: Literal["process", "ingestion"]
    status: Literal["accepted", "running", "retrying", "successful", "failed", "cancelled"]
    attempt: int
    max_attempts: int = Field(serialization_alias="maxAttempts")
    created: datetime
    started: datetime | None
    finished: datetime | None
    progress: JobProgress | None
    request: dict[str, Any]
    result: dict[str, Any] | None
    error: str | None
    cancel_requested: bool = Field(serialization_alias="cancelRequested")
    retry_after: int | None = Field(None, serialization_alias="retryAfter")
    cursor: dict[str, Any] | None
    executor: str

    model_config = ConfigDict(populate_by_name=True)
```

With:

```python
class JobProgress(BaseModel):
    done: int | None
    total: int | None
    percent: float | None
    message: str | None
```

Notes:

- `jobID`, `processID`, `created`, `started`, and `finished` follow OGC naming
- `type`, `attempt`, `maxAttempts`, `request`, `progress`, `result`, `error`, `cancelRequested`, `retryAfter`, `cursor`, and `executor` are native extensions
- `processID` is naturally nullable for non-process jobs
- `cursor` is framework-owned storage for process-specific resume checkpoints
- internal model fields should stay snake_case; OGC-facing names should be produced through serialization aliases
- `cursor` is intended for lightweight, coarse-grained checkpoint state only; large resume metadata belongs in a sidecar file rather than in `jobs.json`
- `retryAfter` represents a delay in seconds, matching the HTTP `Retry-After` semantics rather than an absolute timestamp

## Dispatcher and Executor Model

Introduce one native dispatcher/runtime that:

1. creates job record with `accepted`
2. submits worker task
3. marks `running`
4. executes target callable
5. stores `result` or `error`
6. marks final status

For the MVP:

- use `ThreadPoolExecutor`
- persist state to `jobs.json`
- do not block the HTTP request when async mode is requested

This MVP has now been implemented.

But design it around a small executor abstraction, not a hardcoded thread pool:

```python
class ProcessExecutor(Protocol):
    kind: str
    def submit(
        self,
        job_id: str,
        func: Callable[..., Any],
        kwargs: dict[str, Any],
        *,
        max_retries: int = 0,
    ) -> None: ...
```

This keeps the first implementation simple while leaving room for:

- subprocess-backed execution
- plugin-provided execution
- delegated/distributed execution later

`kind: str` is intentionally a lightweight executor identifier rather than a behavioral method. It is primarily there so the job record can capture what runner/executor was used.

## Function Contract Guidance

Do not force a large callable-contract refactor in the first `#111` implementation, but do converge toward one shared execution contract.

Current process execution works via:

```python
func(**request)
```

Recommendation:

- keep current process callable shape working
- add optional progress plumbing without breaking the current contract
- add cooperative cancellation and resume hooks with safe no-op defaults
- avoid coupling the first job implementation to a large `ProcessContext` object unless it becomes necessary immediately

Suggested target signature style:

```python
def run_process(
    *,
    on_progress: ProgressCallback = lambda *_: None,
    is_cancel_requested: Callable[[], bool] = lambda: False,
    load_cursor: Callable[[], dict[str, Any] | None] = lambda: None,
    save_cursor: Callable[[dict[str, Any]], None] = lambda _cursor: None,
    **kwargs: Any,
) -> dict[str, Any]:
    ...
```

This keeps direct local/test invocation simple while giving the job framework what it needs.

## Ingestion and Process Sequencing

Although the long-term target is one shared model for process execution, ingestion, and sync, the first implementation should start with **process jobs first**.

Why:

- `/processes` is already the cleanest native abstraction after `689`
- `resample` is a real built-in process to exercise the path
- ingestion migration is broader and touches more operational code paths

Then bring ingestion onto the same job layer once the core job store, executor abstraction, and runtime semantics are proven.

## Cancellation, Retry, and Resume

These are framework concerns, but the framework should only provide **cooperative** semantics.

The framework owns:

- API surface
- state transitions
- persistence of retry/cancel/recovery metadata
- attempt counting and backoff
- restart recovery policy

Individual processes own:

- safe checkpoint boundaries
- what cursor state to save
- whether work is retryable and resumable in practice
- where cancellation checks happen

### Cancellation

Cancellation should not try to kill worker threads forcefully.

Instead:

- `DELETE /jobs/{job_id}` sets `cancelRequested = true`
- the running process checks `is_cancel_requested()`
- the process exits cleanly at a safe boundary
- the framework marks the job `cancelled`

The job record should be retained after cancellation for auditability and later inspection. Cancellation is not the same as deleting/dismissing the record.

This is important for download/build workflows where abrupt termination risks corrupt intermediates.

### Retry

Framework behavior:

- bounded retry count
- exponential backoff, with the original `#111` issue values (`60s`, `120s`, `240s`) as the initial policy
- minimal retryable/non-retryable exception classification
- persisted `attempt` and `retryAfter`

The current implementation now uses:

- `retryAfter` as delay seconds
- `60s`, `120s`, `240s` retry delays

Retry classification remains intentionally simple for now.

### Resume

Differentiate:

- download resume
- Zarr build resume
- restart recovery of interrupted jobs

True resumable Zarr build depends on `#64` and should not be implied by the first async-jobs PR.

For framework restart recovery:

- jobs persisted as `accepted`, `running`, or `retrying` should be examined on startup
- `accepted` is a distinct case: the job was created but may never have been picked up before process exit
- resumable jobs can be requeued
- non-resumable jobs can be marked failed with a restart-related error

This is different from true workflow checkpointing, but it gives the framework a coherent recovery story.

The framework-level restart recovery is now implemented. True meaningful resume for large ingest/build workflows still depends on `#64`.

## Recommended Jira Ticket

### Title

Implement native async job framework for process execution

### Summary

Build the native job framework on top of the `CLIM-689` process surface. Add persisted jobs, async execution via `Prefer: respond-async`, job polling, framework-level cancellation/retry/recovery semantics, and a pluggable executor abstraction. Treat `CLIM-689` as already delivered; this ticket should not reopen process registry or `/processes` design.

### Acceptance scope

- persisted `jobs.json` store
- job schemas and mutation helpers
- `GET /jobs`
- `GET /jobs/{job_id}`
- `GET /jobs/{job_id}/results` explicitly deferred for the first slice while `result` remains embedded in `GET /jobs/{job_id}`
- `POST /processes/{id}/execution` supports async negotiation with `Prefer: respond-async`
- `202 Accepted` + `Location` header for async mode
- synchronous fallback remains supported
- `DELETE /jobs/{job_id}` for cooperative cancellation
- retry state and attempt tracking in the framework
- startup recovery policy for interrupted jobs
- executor abstraction with default in-process implementation
- built-in `resample` works through the async path

This acceptance scope is now satisfied for the first slice.

### Explicitly not in this ticket

- ingestion migration
- `/sync` migration
- distributed worker backends
- full resumable zarr build without `#64`
