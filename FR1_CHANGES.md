# FR1 lifecycle modification

- Added `JobStatus` enum: `JOB_NEW`, `JOB_READY`, `JOB_RUNNING`, `JOB_COMPLETED`.
- Added `status` to `Job`.
- Initialized each job to `JOB_READY` at time 0 if its arrival is 0, otherwise `JOB_NEW`.
- Marked jobs `JOB_READY` when their simulated arrival time is reached.
- Marked selected jobs `JOB_RUNNING` during FCFS/Priority execution and each Round Robin time slice.
- Marked completed jobs `JOB_COMPLETED` when their remaining burst reaches zero.
- Marked preempted Round Robin jobs `JOB_READY` before requeueing.
- Existing scheduling output and commands remain unchanged; status is internal tracking.
- Also corrected `RLIMIT_FSIZE`: the initial write can legally be short; a checked follow-up write then demonstrates `SIGXFSZ`.

Build and test with `make clean && make && make check`.
