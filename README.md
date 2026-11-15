# CampusCompute — Intelligent Job Scheduling and Resource Management

CampusCompute is a command-line operating systems mini-project written in C. It demonstrates CPU scheduling, job lifecycle tracking, producer-consumer synchronization, interprocess communication, deadlock avoidance, worker-process monitoring, and resource limits on Linux.

**Platform:** Ubuntu / Linux / WSL Ubuntu  
**Language:** C11 with POSIX APIs

The program runs its demonstrations locally in the terminal.

## Prerequisites

On Ubuntu or WSL Ubuntu, install the compiler and build tools:

```bash
sudo apt update
sudo apt install build-essential make
```

## Build and run

From the project directory, compile the program and explore its commands:

```bash
make clean && make
./campuscompute help
./campuscompute all
```

For the interactive menu, run `./campuscompute menu`.

## Run tests

```bash
make check
```

The smoke-test script runs the main demonstrations and checks expected outputs and invalid-input handling.

## Feature commands

| Requirement | Command | Demonstration |
|---|---|---|
| FR1 / FR2 | `./campuscompute fcfs --file samples/jobs.csv` | FCFS, validated jobs, lifecycle states, timeline, Gantt, per-job CT/TAT/WT, utilisation |
| FR2 | `./campuscompute rr --quantum 3 --file samples/jobs.csv` | Preemptive RR with ready-queue order and quantum |
| FR2 | `./campuscompute priority --file samples/jobs.csv` | Non-preemptive priority |
| FR2 | `./campuscompute compare --quantum 3 --file samples/jobs.csv` | Same workload, three policies |
| FR3 | `./campuscompute logger` | Two producers, one consumer, 4-entry queue, mutex, condition variables, clean joins |
| FR4 | `./campuscompute ipc` | POSIX shared memory, two named semaphores, fork, timeout, unlink |
| FR5 | `./campuscompute banker safe` | Built-in safe case |
| FR5 | `./campuscompute banker unsafe` | Built-in unsafe case |
| FR6 | `./campuscompute worker normal` | fork + waitpid + exit status 0 |
| FR6 | `./campuscompute worker fail` | Deliberately simulated exit code 42, detected by parent |
| FR7 | `./campuscompute limit` | RLIMIT_FSIZE applied inside child; SIGXFSZ detected by parent |
| FR7 | `./campuscompute cleanup` | Removes generated log, verifies its absence, and checks /dev/shm for leftover CampusCompute IPC objects |
| FR8 | `./campuscompute menu` | Optional interactive menu |
| FR8 | `./campuscompute bogus` | Invalid command error (nonzero exit status) |
| FR8 | `./campuscompute fcfs --file samples/jobs_invalid.csv` | Invalid CSV error (nonzero exit status) |
| Integrated | `./campuscompute all` | All demos; summary of any failures |

## Scheduling policy rules

- FCFS: earlier arrival first; ties by smaller job ID.
- Priority: smaller priority number first; ties by earlier arrival then smaller job ID.
- RR: FIFO queue; ties by arrival then smaller ID; newly arrived jobs enter before the just-preempted job is appended; default quantum is 2.
- CT is completion time, TAT = CT - arrival, WT = TAT - burst, CPU utilisation = total CPU burst / makespan x 100%.
- Timeline and Gantt chart show idle intervals where applicable. The timeline labels each execution interval as RUNNING -> READY (RR preemption) or RUNNING -> COMPLETED; result tables show each job's status at time zero and its final status. Comparison shows completed-job counts.
- All job metrics are calculated, not hard-coded.

## Job CSV format

Each line is `id,arrival,burst,priority`, with an optional header. Example:

```csv
id,arrival,burst,priority
1,0,5,2
2,1,3,1
3,2,4,3
4,4,2,1
```

IDs unique and positive, arrivals nonnegative, CPU bursts positive, priorities nonnegative. Maximum 64 jobs.

## Cleanup and important cautions

- The logger creates `campuscompute.log` in the working directory. `cleanup` removes that log and verifies its absence, then scans `/dev/shm` for CampusCompute-specific shared-memory and semaphore names without deleting them. Back up an existing log before running cleanup if you need to keep it.
- The IPC demonstration uses names based on its PID; it unlinks only names it created. The child is reaped by `waitpid`, and threads are joined. IPC waits for the response for at most five seconds.
- The resource-limit demo uses a unique `/tmp` file and removes it afterward. The `SIGXFSZ` termination is an expected outcome **for that child**, not an actual system fault.
- The simulated worker failure is intentional and explicitly labelled, not a real OS failure.
- Do not run `make clean` until you are ready to delete the built executable.

## Code map

- `main.c`: scheduling, CLI/menu, threads, IPC, built-in Banker examples, workers, limits, integration.
- `Makefile`: reproducible source build.
- `samples/`: job and invalid-input examples.
- `test_commands.sh`: repeatable smoke tests (`make check`).

