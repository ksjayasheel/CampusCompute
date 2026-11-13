# FR1 lifecycle modification

- Added `JobStatus` enum: `JOB_NEW`, `JOB_READY`, `JOB_RUNNING`, `JOB_COMPLETED`.
- Added `status` to `Job`.
- Initialized each job to `JOB_READY` at time 0 if its arrival is 0, otherwise `JOB_NEW`.
- Marked jobs `JOB_READY` when their simulated arrival time is reached.
- Marked selected jobs `JOB_RUNNING` during FCFS/Priority execution and each Round Robin time slice.
- Marked completed jobs `JOB_COMPLETED` when their remaining burst reaches zero.
- Marked preempted Round Robin jobs `JOB_READY` before requeueing.
- FCFS, RR, and Priority now show initial and final job status, completed-job counts, and RUNNING -> READY/COMPLETED transitions in their timelines.
- Comparison includes a completed-job count for each policy; existing scheduling commands remain compatible.
- `make check` verifies status display and RR preemption transitions.
- Also corrected `RLIMIT_FSIZE`: the initial write can legally be short; a checked follow-up write then demonstrates `SIGXFSZ`.

Build and test with `make clean && make && make check`.
