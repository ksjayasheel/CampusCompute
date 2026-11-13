# CampusCompute — OS Level 3 Mini-Project (Improved)

**Platform:** Ubuntu / Linux / WSL Ubuntu. **Language:** C11 and POSIX APIs.
This is source code, not a fabricated screenshot submission. Execute commands on your own terminal.

## Build and quick start

```bash
sudo apt update && sudo apt install build-essential make unzip
make clean && make
./campuscompute help
./campuscompute all
make check
```

To navigate interactively, run `./campuscompute menu`.

## Feature commands

| Requirement | Command | Demonstration |
|---|---|---|
| FR1 / FR2 | `./campuscompute fcfs --file samples/jobs.csv` | FCFS, validated jobs, timeline, Gantt, per-job CT/TAT/WT, utilisation |
| FR2 | `./campuscompute rr --quantum 3 --file samples/jobs.csv` | Preemptive RR with ready-queue order and quantum |
| FR2 | `./campuscompute priority --file samples/jobs.csv` | Non-preemptive priority |
| FR2 | `./campuscompute compare --quantum 3 --file samples/jobs.csv` | Same workload, three policies |
| FR3 | `./campuscompute logger` | Two producers, one consumer, 4-entry queue, mutex, condition variables, clean joins |
| FR4 | `./campuscompute ipc` | POSIX shared memory, two named semaphores, fork, timeout, unlink |
| FR5 | `./campuscompute banker safe` | Built-in safe case |
| FR5 | `./campuscompute banker unsafe` | Built-in unsafe case |
| FR5 | `./campuscompute banker samples/banker_safe.txt` | User-supplied resource matrices |
| FR5 | `./campuscompute banker samples/banker_unsafe.txt` | User-supplied unsafe matrices |
| FR6 | `./campuscompute worker normal` | fork + waitpid + exit status 0 |
| FR6 | `./campuscompute worker fail` | Deliberately simulated exit code 42, detected by parent |
| FR7 | `./campuscompute limit` | RLIMIT_FSIZE applied inside child; SIGXFSZ detected by parent |
| FR7 | `./campuscompute cleanup` | Deletes program-created log; IPC is unlinked by IPC demo |
| FR8 | `./campuscompute menu` | Optional interactive menu |
| FR8 | `./campuscompute bogus` | Invalid command error (nonzero exit status) |
| FR8 | `./campuscompute fcfs --file samples/jobs_invalid.csv` | Invalid CSV error (nonzero exit status) |
| Integrated | `./campuscompute all` | All demos; summary of any failures |

## Scheduling policy rules

- FCFS: earlier arrival first; ties by smaller job ID.
- Priority: smaller priority number first; ties by earlier arrival then smaller job ID.
- RR: FIFO queue; ties by arrival then smaller ID; newly arrived jobs enter before the just-preempted job is appended; default quantum is 2.
- CT is completion time, TAT = CT - arrival, WT = TAT - burst, CPU utilisation = total CPU burst / makespan x 100%.
- Timeline and Gantt chart show idle intervals where applicable. All job metrics are calculated, not hard-coded.

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

## Banker's custom input format

Whitespace-separated integers, **without a header**:

1. `process_count resource_count` (max 32 processes / 16 resource types)
2. `resource_count` integers for **Available**
3. `process_count` lines of `resource_count` integers for **Allocation**
4. `process_count` lines of `resource_count` integers for **Maximum**

The program validates `Maximum >= Allocation` for every entry, calculates **Need = Maximum - Allocation**, then runs the safety algorithm. Unsafe resource states are valid inputs; the command exits successfully while reporting UNSAFE. Run the two files in `samples/` as examples.

## Screenshot evidence: S01 to S24

The PDF submission is evidence of *your own terminal*, not part of the source ZIP. Before screenshots:

```bash
export SRN='REPLACE_WITH_YOUR_FULL_SRN'
export PS1='[$SRN] \w\$ '
```

Capture: S01 `pwd; uname -a`; S02 `gcc --version; make --version`; S03 `ls -l`; S04 `make clean && make`; S05 `./campuscompute help`; S06 FCFS; S07 RR; S08 Priority; S09 compare; S10-S11 logger (including summary); S12-S13 IPC and the unlink output; S14-S15 Banker safe/unsafe; S16-S17 worker normal/fail; S18 limit; S19 invalid input; S20 cleanup and `ls /dev/shm | grep campuscompute || echo 'no CampusCompute IPC objects'`; S21 integrated run; S22 `head -n 12 campuscompute.log` (run `all` AFTER cleanup); S23 `ls -l`; S24 `unzip -l CampusCompute_Improved.zip; unzip -t CampusCompute_Improved.zip` (adapt archive location as needed).

Each screenshot must show the **full actual SRN in the terminal prompt**, with a one-sentence caption. Combine screenshots into one PDF named with your own SRN; do not fake or reuse screenshots.

## Cleanup and important cautions

- The logger creates `campuscompute.log` in the working directory; `cleanup` only unlinks this named log. Back up an existing log before running it if you need to keep it.
- The IPC demonstration uses names based on its PID; it unlinks only names it created. The child is reaped by `waitpid`, and threads are joined. IPC waits for the response for at most five seconds.
- The resource-limit demo uses a unique `/tmp` file and removes it afterward. The `SIGXFSZ` termination is an expected outcome **for that child**, not an actual system fault.
- The simulated worker failure is intentional and explicitly labelled, not a real OS failure.
- Do not run `make clean` until you are ready to delete the built executable.

## Code map

- `main.c`: scheduling, CLI/menu, threads, IPC, built-in Banker examples, workers, limits, integration.
- `banker_custom.c`: external matrix parsing and Banker's safety algorithm.
- `Makefile`: reproducible source build.
- `samples/`: jobs and safe/unsafe/invalid input examples.
- `test_commands.sh`: repeatable smoke tests (`make check`).

## Sources of ideas

The reference screenshot PDFs show techniques such as Gantt output, process counts, and custom resource states; their original C source code was **not supplied**. This project is an independent improvement of the existing `main.c`, not a claim that those PDF sources were audited or reused.
