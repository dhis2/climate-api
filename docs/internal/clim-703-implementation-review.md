# Review: CLIM-703 Implementation

Reviewed against `docs/internal/issue-111-async-jobs-design.md`, GitHub issues `#111` and `#64`,
and the current branch state after the final cleanup pass.

---

## Outcome

`CLIM-703` is ready for PR.

The native async job framework slice described in `#111` is implemented, lint-clean, and covered
by the current test suite.

Validation signal on this branch:

- `make lint` passes
- `make test` passes
- result: `247 passed, 1 skipped`

---

## Acceptance scope

Every item marked in scope for the first slice is delivered:

| Requirement | Status |
|---|---|
| `jobs.json` store under `data_dir` | ✓ |
| Job schemas and mutation helpers | ✓ |
| `GET /jobs` | ✓ |
| `GET /jobs/{job_id}` | ✓ |
| `GET /jobs/{job_id}/results` explicitly deferred | ✓ |
| `POST /processes/{id}/execution` async negotiation | ✓ |
| `202 Accepted` + `Location` header | ✓ |
| Synchronous fallback | ✓ |
| `DELETE /jobs/{job_id}` cooperative cancellation | ✓ |
| Retry state and attempt tracking | ✓ |
| Startup recovery | ✓ |
| Executor abstraction | ✓ |
| `resample` wired through async path | ✓ |

---

## Design divergences worth keeping in mind

These are not blockers for the PR.

- `ProcessExecutor.submit(...)` follows the `concurrent.futures.Executor` shape rather than the
  earlier design sketch. This is a better split of concerns: retry and job semantics remain in the
  runtime.
- `JobRecord.process_id` is currently non-nullable. That is fine for the current process-backed
  job model, but future non-process job types may require a migration.
- `error` is represented as a structured `JobError` object rather than a plain string. This is an
  improvement over the original design note.

---

## Remaining non-blocking follow-up topics

- `time.sleep(retry_after)` still occupies a thread-pool worker during retry delay. That is
  acceptable for the current MVP but should be revisited before adding higher-concurrency
  ingestion jobs.
- `resample.yaml` method enum values and Python-side supported-method constants are still kept in
  sync manually.
- `JobRecord.process_id` should be revisited when ingestion and other non-process job types move
  onto the framework.

---

## Relationship to `#64`

`CLIM-703` and `#64` address different layers of the same problem and compose cleanly.

### What `CLIM-703` now owns

- persisted job lifecycle
- submission / transition / retry / recovery orchestration
- progress reporting
- cooperative cancellation
- cursor/checkpoint hooks in the runtime contract

### What `#64` still owns

- meaningful data-plane checkpoints
- incremental/resumable materialization behaviour
- efficient resume after interruption during large ingest/build workflows

### Current meaning of the `resample` integration

`resample` now runs through the native job framework and declares the cursor hooks explicitly, even
though it does not use them yet. That makes the contract visible and keeps the runtime shape ready
for future resumable processes.

Until a resumable ingest/build implementation lands, restart recovery correctly re-runs interrupted
jobs from the beginning.
