#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <semaphore.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>

#define MAX_JOBS 64
#define MAX_EVENTS 100000
#define LOG_CAP 4
#define LOG_ITEMS 12
#define LOG_FILE "campuscompute.log"

/* FR1: Simulated lifecycle of each scheduling job (not a Linux process). */
typedef enum {
    JOB_NEW,
    JOB_READY,
    JOB_RUNNING,
    JOB_COMPLETED
} JobStatus;

typedef struct {
    int id, arrival, burst, priority, remaining, completion;
    JobStatus status;
} Job;
typedef struct {int job, start, end;} Event;
typedef struct {Event ev[MAX_EVENTS]; int n;} Timeline;
typedef struct {double avg_wait, avg_turn, utilization; int makespan, busy, completed;} Stats;
int banker_custom(const char *path);

static void usage(const char *prog) {
    printf("CampusCompute - OS Level 3 mini-project\n"
           "Usage: %s <command> [options]\n"
           "  help                         Show this help\n"
           "  fcfs [--file jobs.csv]       First Come First Served\n"
           "  rr [--quantum N] [--file jobs.csv] Round Robin (default quantum=2)\n"
           "  priority [--file jobs.csv]   Non-preemptive priority\n"
           "  compare [--quantum N] [--file jobs.csv] Compare same workload\n"
           "  logger                       Bounded POSIX producer-consumer logging\n"
           "  ipc                          POSIX shared memory + named semaphores\n"
           "  banker safe|unsafe|FILE      Banker's safety analysis (custom state file)\n"
           "  worker normal|fail           fork/waitpid worker demonstration\n"
           "  limit                        setrlimit(RLIMIT_FSIZE) demonstration\n"
           "  cleanup                      Remove this project's generated log\n"
           "  all                          Run all demos, including safe/unsafe cases\n"
           "  menu                         Interactive demo menu\n"
           "\nJobs CSV: id,arrival,burst,priority (header optional).\n"
           "Rules: lower priority number wins; ties: earlier arrival, then smaller ID.\n"
           "RR: FIFO ready queue, arrivals are queued before an expired job is requeued.\n",
           prog);
}
static int parse_int(const char *s, int min, int max, int *value) {
    if(!s || !*s) return -1;
    errno=0; char *end=NULL; long v=strtol(s,&end,10);
    if(errno || *end || end==s || v<min || v>max) return -1;
    *value=(int)v; return 0;
}
static void default_jobs(Job *j,int *n) {
    Job d[]={{1,0,5,2,5,-1,JOB_READY},{2,1,3,1,3,-1,JOB_NEW},{3,2,4,3,4,-1,JOB_NEW},{4,4,2,1,2,-1,JOB_NEW}};
    *n=4; memcpy(j,d,sizeof(d));
}
static int load_jobs(const char *path, Job *j, int *n) {
    if(!path) {default_jobs(j,n); return 0;}
    FILE *f=fopen(path,"r"); if(!f){perror("open jobs file");return -1;}
    char line[256]; *n=0; int line_no=0, header=0;
    while(fgets(line,sizeof(line),f)) {
        ++line_no;
        if(!strchr(line,'\n') && !feof(f)){fprintf(stderr,"Line %d too long\n",line_no);fclose(f);return -1;}
        char *p=line; while(*p==' '||*p=='\t') ++p;
        if(*p=='\n'||*p=='#'||*p=='\0')continue;
        if(!header && *n==0 && strncmp(p,"id,",3)==0){header=1;continue;}
        if(*n==MAX_JOBS){fprintf(stderr,"Maximum %d jobs\n",MAX_JOBS);fclose(f);return -1;}
        /* Split exactly four CSV columns, retaining empty fields as errors. */
        char *parts[4]={0}; int c=0; char *cursor=p;
        for(;c<4;c++){
            parts[c]=cursor;
            char *comma=strchr(cursor,',');
            if(c<3){if(!comma)break;*comma='\0';cursor=comma+1;}
            else if(comma){c=5;break;}
        }
        if(c!=4){fprintf(stderr,"Invalid CSV at line %d: expected 4 nonempty columns\n",line_no);fclose(f);return -1;}
        for(int z=0;z<4;z++){
            char *end=parts[z]+strlen(parts[z]);
            while(end>parts[z] && (end[-1]=='\n'||end[-1]=='\r'||end[-1]==' '||end[-1]=='\t'))*--end='\0';
            while(*parts[z]==' '||*parts[z]=='\t')parts[z]++;
            if(!*parts[z]){fprintf(stderr,"Empty column at line %d\n",line_no);fclose(f);return -1;}
        }
        int vals[4]; for(int k=0;k<4;k++){
            int lo=(k==0||k==2)?1:0;
            int hi=(k==1||k==2)?100000:1000000;
            if(parse_int(parts[k],lo,hi,&vals[k])){
                fprintf(stderr,"Invalid number at line %d column %d\n",line_no,k+1);fclose(f);return -1;
            }
        }
        for(int k=0;k<*n;k++)if(j[k].id==vals[0]){
            fprintf(stderr,"Duplicate job ID %d\n",vals[0]);fclose(f);return -1;
        }
        j[*n]=(Job){vals[0],vals[1],vals[2],vals[3],vals[2],-1,vals[1]==0?JOB_READY:JOB_NEW}; ++*n;
    }
    if(ferror(f)){perror("read jobs");fclose(f);return -1;}
    fclose(f);
    if(*n==0){fprintf(stderr,"No jobs found\n");return -1;}
    return 0;
}
static int add_event(Timeline *t,int job,int start,int end) {
    if(end<=start)return 0;
    if(t->n && t->ev[t->n-1].job==job && t->ev[t->n-1].end==start){t->ev[t->n-1].end=end;return 0;}
    if(t->n>=MAX_EVENTS){fprintf(stderr,"Timeline exceeds %d events\n",MAX_EVENTS);return -1;}
    t->ev[t->n++]=(Event){job,start,end};return 0;
}
static int choose(Job *j,int n,int time,int method) {
    int best=-1;
    for(int i=0;i<n;i++) {
        if(j[i].remaining<=0 || j[i].arrival>time)continue;
        if(best<0 || (method==0 ? (j[i].arrival<j[best].arrival || (j[i].arrival==j[best].arrival && j[i].id<j[best].id)) :
            (j[i].priority<j[best].priority || (j[i].priority==j[best].priority && (j[i].arrival<j[best].arrival || (j[i].arrival==j[best].arrival && j[i].id<j[best].id)))))) best=i;
    }
    return best;
}
static int next_arrival(Job *j,int n,int time){int next=INT_MAX;for(int i=0;i<n;i++)if(j[i].remaining>0&&j[i].arrival>time&&j[i].arrival<next)next=j[i].arrival;return next;}
static void sort_order(Job *j,int *order,int n){
    for(int i=0;i<n;i++)order[i]=i;
    for(int i=1;i<n;i++){int k=order[i],a=i-1;while(a>=0&&(j[order[a]].arrival>j[k].arrival||(j[order[a]].arrival==j[k].arrival&&j[order[a]].id>j[k].id))){order[a+1]=order[a];--a;}order[a+1]=k;}
}
/* A job becomes READY when its arrival time is reached. */
static void update_arrivals(Job *jobs, int n, int now) {
    for (int i=0; i<n; i++) {
        if (jobs[i].status == JOB_NEW && jobs[i].arrival <= now)
            jobs[i].status = JOB_READY;
    }
}

static int schedule(Job *j,int n,int method,int quantum,Timeline *tl) {
    memset(tl,0,sizeof(*tl));
    for(int i=0;i<n;i++){
        j[i].remaining=j[i].burst;
        j[i].completion=-1;
        j[i].status=(j[i].arrival==0)?JOB_READY:JOB_NEW;
    }
    int time=0,done=0;
    if(method!=1){
        while(done<n){int k=choose(j,n,time,method);
            if(k<0){int next=next_arrival(j,n,time);if(next==INT_MAX)return -1;if(add_event(tl,-1,time,next))return -1;time=next;update_arrivals(j,n,time);continue;}
            j[k].status=JOB_RUNNING;int end=time+j[k].remaining;if(add_event(tl,j[k].id,time,end))return -1;
            time=end;j[k].remaining=0;j[k].completion=time;j[k].status=JOB_COMPLETED;done++;update_arrivals(j,n,time);
        }
        return 0;
    }
    /* Fixed-capacity circular queue: each job is enqueued once per ready-queue turn. */
    int q[MAX_JOBS+1],head=0,tail=0,count=0,order[MAX_JOBS],next=0;
    sort_order(j,order,n);
    while(done<n){
        while(next<n&&j[order[next]].arrival<=time){int a=order[next++];j[a].status=JOB_READY;q[tail]=a;tail=(tail+1)%(MAX_JOBS+1);count++;}
        if(!count){int t=j[order[next]].arrival;if(add_event(tl,-1,time,t))return -1;time=t;update_arrivals(j,n,time);continue;}
        int k=q[head];head=(head+1)%(MAX_JOBS+1);count--;j[k].status=JOB_RUNNING;
        int slice=j[k].remaining<quantum?j[k].remaining:quantum;
        if(add_event(tl,j[k].id,time,time+slice))return -1;
        time+=slice;j[k].remaining-=slice;
        while(next<n&&j[order[next]].arrival<=time){int a=order[next++];j[a].status=JOB_READY;q[tail]=a;tail=(tail+1)%(MAX_JOBS+1);count++;}
        if(j[k].remaining){j[k].status=JOB_READY;q[tail]=k;tail=(tail+1)%(MAX_JOBS+1);count++;}
        else {j[k].completion=time;j[k].status=JOB_COMPLETED;done++;}
    }
    return 0;
}
static const char *job_status_name(JobStatus status) {
    switch (status) {
        case JOB_NEW: return "NEW";
        case JOB_READY: return "READY";
        case JOB_RUNNING: return "RUNNING";
        case JOB_COMPLETED: return "COMPLETED";
    }
    return "UNKNOWN";
}
static Stats stats(Job *j,int n) {
    Stats s={0};
    for(int i=0;i<n;i++) {int tat=j[i].completion-j[i].arrival;int wait=tat-j[i].burst;s.avg_turn+=tat;s.avg_wait+=wait;if(j[i].completion>s.makespan)s.makespan=j[i].completion;s.busy+=j[i].burst;if(j[i].status==JOB_COMPLETED)s.completed++;}
    s.avg_turn/=n;s.avg_wait/=n;s.utilization=s.makespan?100.0*s.busy/s.makespan:0;return s;
}
static const char *policy_name(int m){return m==0?"FCFS":m==1?"ROUND ROBIN":"NON-PREEMPTIVE PRIORITY";}
static int schedule_demo(Job *original,int n,int method,int quantum,int verbose,Stats *out){
    Job j[MAX_JOBS];memcpy(j,original,n*sizeof(Job));Timeline tl;
    if(schedule(j,n,method,quantum,&tl)){fprintf(stderr,"Scheduling failed\n");return -1;}
    Stats s=stats(j,n);if(out)*out=s;
    if(!verbose)return 0;
    printf("\n=== %s ===\n",policy_name(method));
    if(method==1)printf("Time quantum = %d\n",quantum);
    printf("Tie-break: arrival time ascending, then job ID ascending; priority: smaller number first.\n");
    puts("Lifecycle: NEW (before arrival) -> READY -> RUNNING -> COMPLETED.");
    if(method==1)puts("Round Robin: unfinished jobs return from RUNNING to READY after a time slice.");
    printf("Timeline (start -> end):\n");
    for(int k=0;k<tl.n;k++){
        if(tl.ev[k].job<0)printf("  %d -> %d : IDLE\n",tl.ev[k].start,tl.ev[k].end);
        else {
            int completed_here=0;
            for(int i=0;i<n;i++)
                if(j[i].id==tl.ev[k].job && j[i].completion==tl.ev[k].end)
                    completed_here=1;
            printf("  %d -> %d : J%d [RUNNING -> %s]\n",
                   tl.ev[k].start,tl.ev[k].end,tl.ev[k].job,
                   completed_here ? "COMPLETED" : "READY");
        }
    }
    printf("Gantt chart: ");
    for(int k=0;k<tl.n;k++)if(tl.ev[k].job<0)printf("|IDLE %d-%d",tl.ev[k].start,tl.ev[k].end);else printf("|J%d %d-%d",tl.ev[k].job,tl.ev[k].start,tl.ev[k].end);
    puts("|");
    printf("Execution order: ");int shown=0;for(int k=0;k<tl.n;k++)if(tl.ev[k].job>=0){printf("%sJ%d",shown?" -> ":"",tl.ev[k].job);shown++;}puts("");
    printf("%-6s %-8s %-7s %-8s %-11s %-12s %-8s %-10s %-12s\n",
           "Job","Arrival","Burst","Priority","Completion","Turnaround","Waiting","At t=0","Final status");
    for(int i=0;i<n;i++)
        printf("J%-5d %-8d %-7d %-8d %-11d %-12d %-8d %-10s %-12s\n",
               j[i].id,j[i].arrival,j[i].burst,j[i].priority,
               j[i].completion,j[i].completion-j[i].arrival,
               j[i].completion-j[i].arrival-j[i].burst,
               job_status_name(j[i].arrival==0?JOB_READY:JOB_NEW),
               job_status_name(j[i].status));
    printf("Completed jobs: %d/%d\n",s.completed,n);
    printf("Average turnaround = %.2f; average waiting = %.2f; makespan = %d; CPU utilization = %.2f%%\n",s.avg_turn,s.avg_wait,s.makespan,s.utilization);
    return 0;
}
static int compare_demo(Job *j,int n,int quantum){
    Stats s[3];printf("Comparing all policies on SAME %d-job workload (RR quantum=%d)\n",n,quantum);
    for(int m=0;m<3;m++)if(schedule_demo(j,n,m,quantum,0,&s[m]))return -1;
    printf("%-24s %-18s %-18s %-10s %-10s %-12s\n","Policy","Avg turnaround","Avg waiting","Makespan","CPU util","Completed");
    for(int m=0;m<3;m++)printf("%-24s %-18.2f %-18.2f %-10d %9.2f%% %7d/%d\n",
        policy_name(m),s[m].avg_turn,s[m].avg_wait,s[m].makespan,s[m].utilization,s[m].completed,n);
    return 0;
}

/* Bounded POSIX producer-consumer: all queue accesses occur under its mutex. */
typedef struct {int producer,seq;char message[80];} LogItem;
typedef struct {LogItem buf[LOG_CAP];int head,tail,count,active_producers,failed,produced,consumed,wait_full,wait_empty;pthread_mutex_t mu;pthread_cond_t not_full,not_empty;FILE *file;} LogQueue;
typedef struct {LogQueue *q;int producer;} ProducerArg;
static void *producer_thread(void *arg){
    ProducerArg *a=(ProducerArg*)arg;LogQueue *q=a->q;
    for(int i=1;i<=LOG_ITEMS/2;i++){
        LogItem e={.producer=a->producer,.seq=i};snprintf(e.message,sizeof(e.message),"producer=%d event=%d",a->producer,i);
        pthread_mutex_lock(&q->mu);
        while(q->count==LOG_CAP){q->wait_full++;puts("[producer] queue FULL; waiting");pthread_cond_wait(&q->not_full,&q->mu);}
        q->buf[q->tail]=e;q->tail=(q->tail+1)%LOG_CAP;q->count++;q->produced++;
        printf("[producer %d] enqueue event %d, queue=%d/%d\n",a->producer,i,q->count,LOG_CAP);
        pthread_cond_signal(&q->not_empty);pthread_mutex_unlock(&q->mu);
    }
    pthread_mutex_lock(&q->mu);q->active_producers--;pthread_cond_broadcast(&q->not_empty);pthread_mutex_unlock(&q->mu);
    printf("[producer %d] finished cleanly\n",a->producer);return NULL;
}
static void *consumer_thread(void *arg){
    LogQueue *q=(LogQueue*)arg;
    for(;;){
        pthread_mutex_lock(&q->mu);
        while(!q->count && q->active_producers>0){q->wait_empty++;pthread_cond_wait(&q->not_empty,&q->mu);}
        if(!q->count && !q->active_producers){pthread_mutex_unlock(&q->mu);break;}
        LogItem item=q->buf[q->head];q->head=(q->head+1)%LOG_CAP;q->count--;q->consumed++;
        printf("[consumer] dequeue P%d/E%d, queue=%d/%d\n",item.producer,item.seq,q->count,LOG_CAP);
        pthread_cond_signal(&q->not_full);pthread_mutex_unlock(&q->mu);
        struct timespec ts;clock_gettime(CLOCK_REALTIME,&ts);
        if(fprintf(q->file,"%lld.%09ld %s\n",(long long)ts.tv_sec,ts.tv_nsec,item.message)<0)q->failed=1;
    }
    if(fflush(q->file)!=0)q->failed=1;
    printf("[consumer] drained queue and exited cleanly\n");return NULL;
}
static int logger_demo(void){
    printf("\n=== BOUNDED PRODUCER-CONSUMER LOGGER ===\n");
    FILE *f=fopen(LOG_FILE,"w");if(!f){perror("open log");return -1;}
    LogQueue q={.active_producers=2,.file=f};pthread_t producers[2],consumer;ProducerArg args[2]={{&q,1},{&q,2}};
    int rc=pthread_mutex_init(&q.mu,NULL);if(rc){fprintf(stderr,"mutex init: %s\n",strerror(rc));fclose(f);return -1;}
    rc=pthread_cond_init(&q.not_full,NULL);if(rc){fprintf(stderr,"cond init: %s\n",strerror(rc));pthread_mutex_destroy(&q.mu);fclose(f);return -1;}
    rc=pthread_cond_init(&q.not_empty,NULL);if(rc){fprintf(stderr,"cond init: %s\n",strerror(rc));pthread_cond_destroy(&q.not_full);pthread_mutex_destroy(&q.mu);fclose(f);return -1;}
    /* On thread-creation errors, maintain producer count so consumer can terminate. */
    int consumer_started=0,started=0;
    rc=pthread_create(&consumer,NULL,consumer_thread,&q);
    if(rc){fprintf(stderr,"consumer create: %s\n",strerror(rc));goto finish;}
    consumer_started=1;
    for(int i=0;i<2;i++){
        rc=pthread_create(&producers[i],NULL,producer_thread,&args[i]);
        if(rc){fprintf(stderr,"producer create: %s\n",strerror(rc));break;}started++;
    }
    if(started<2){pthread_mutex_lock(&q.mu);q.active_producers-=2-started;pthread_cond_broadcast(&q.not_empty);pthread_mutex_unlock(&q.mu);}
    for(int i=0;i<started;i++)pthread_join(producers[i],NULL);
    if(consumer_started)pthread_join(consumer,NULL);
finish:
    pthread_cond_destroy(&q.not_empty);pthread_cond_destroy(&q.not_full);pthread_mutex_destroy(&q.mu);
    if(fclose(f)!=0){perror("close log");return -1;}
    if(rc||q.failed){fprintf(stderr,"Logger did not complete successfully\n");return -1;}
    printf("All threads joined: produced=%d consumed=%d pending=%d full_waits=%d empty_waits=%d\n",q.produced,q.consumed,q.count,q.wait_full,q.wait_empty);
    printf("Generated log: %s\n",LOG_FILE);
    return (q.produced==LOG_ITEMS && q.consumed==LOG_ITEMS && q.count==0)?0:-1;
}

typedef struct {int input,result;char message[96];} SharedBlock;
static int ipc_demo(void){
    printf("\n=== POSIX SHARED MEMORY IPC + NAMED SEMAPHORES ===\n");
    char shm_name[64],request_name[64],response_name[64];
    snprintf(shm_name,sizeof(shm_name),"/campuscompute_%ld",(long)getpid());
    snprintf(request_name,sizeof(request_name),"/cc_request_%ld",(long)getpid());
    snprintf(response_name,sizeof(response_name),"/cc_response_%ld",(long)getpid());
    int fd=-1,ret=-1;SharedBlock *block=MAP_FAILED;sem_t *req=SEM_FAILED,*resp=SEM_FAILED;pid_t pid=-1;
    fd=shm_open(shm_name,O_CREAT|O_EXCL|O_RDWR,0600);if(fd<0){perror("shm_open");goto cleanup;}
    if(ftruncate(fd,sizeof(SharedBlock))<0){perror("ftruncate");goto cleanup;}
    block=mmap(NULL,sizeof(SharedBlock),PROT_READ|PROT_WRITE,MAP_SHARED,fd,0);if(block==MAP_FAILED){perror("mmap");goto cleanup;}
    memset(block,0,sizeof(*block));
    req=sem_open(request_name,O_CREAT|O_EXCL,0600,0);if(req==SEM_FAILED){perror("sem_open request");goto cleanup;}
    resp=sem_open(response_name,O_CREAT|O_EXCL,0600,0);if(resp==SEM_FAILED){perror("sem_open response");goto cleanup;}
    fflush(NULL);pid=fork();if(pid<0){perror("fork");goto cleanup;}
    if(pid==0){
        int e;do{e=sem_wait(req);}while(e<0&&errno==EINTR);
        if(e<0)_exit(2);
        block->result=block->input*block->input;
        snprintf(block->message,sizeof(block->message),"Child %ld squared %d",(long)getpid(),block->input);
        if(sem_post(resp)<0)_exit(3);
        _exit(0);
    }
    block->input=7;printf("Parent %ld wrote input=%d to POSIX shared memory\n",(long)getpid(),block->input);
    if(sem_post(req)<0){perror("sem_post request");goto cleanup;}
    struct timespec deadline;
    if(clock_gettime(CLOCK_REALTIME,&deadline)<0){perror("clock_gettime");goto cleanup;}
    deadline.tv_sec+=5;
    int e;do{e=sem_timedwait(resp,&deadline);}while(e<0&&errno==EINTR);
    if(e<0){perror("sem_timedwait response (5-second timeout)");goto cleanup;}
    printf("Parent read: result=%d; message='%s'\n",block->result,block->message);
    ret=0;
cleanup:
    if(pid>0){int status;if(ret!=0)kill(pid,SIGTERM);if(waitpid(pid,&status,0)<0){perror("waitpid");ret=-1;}else if(!WIFEXITED(status)||WEXITSTATUS(status)!=0){fprintf(stderr,"IPC child terminated unsuccessfully\n");ret=-1;}else printf("IPC child reaped successfully\n");}
    if(resp!=SEM_FAILED)sem_close(resp);
    if(req!=SEM_FAILED)sem_close(req);
    if(block!=MAP_FAILED)munmap(block,sizeof(*block));
    if(fd>=0)close(fd);
    /* Only objects created with this process-specific name are unlinked. */
    if(resp!=SEM_FAILED){if(sem_unlink(response_name)<0)perror("sem_unlink response");else puts("Response semaphore unlinked");}
    if(req!=SEM_FAILED){if(sem_unlink(request_name)<0)perror("sem_unlink request");else puts("Request semaphore unlinked");}
    if(fd>=0){if(shm_unlink(shm_name)<0)perror("shm_unlink");else puts("Shared-memory object unlinked");}
    return ret;
}

static int banker_demo(int unsafe){
    enum {P=5,R=3};
    int alloc[P][R]={{0,1,0},{2,0,0},{3,0,2},{2,1,1},{0,0,2}};
    int max[P][R]={{7,5,3},{3,2,2},{9,0,2},{2,2,2},{4,3,3}};
    int avail[R]={3,3,2};if(unsafe)for(int r=0;r<R;r++)avail[r]=0;
    int need[P][R],work[R],finish[P]={0},seq[P],len=0;
    for(int r=0;r<R;r++)work[r]=avail[r];
    printf("\n=== BANKER'S %s STATE ===\n",unsafe?"UNSAFE":"SAFE");
    printf("Available: [%d %d %d]\n",avail[0],avail[1],avail[2]);
    puts("Job  Allocation  Maximum     Need");
    for(int i=0;i<P;i++){
        for(int r=0;r<R;r++){if(max[i][r]<alloc[i][r]){fprintf(stderr,"Invalid resource matrix\n");return -1;}need[i][r]=max[i][r]-alloc[i][r];}
        printf("P%-3d [%d %d %d]   [%d %d %d]   [%d %d %d]\n",i,alloc[i][0],alloc[i][1],alloc[i][2],max[i][0],max[i][1],max[i][2],need[i][0],need[i][1],need[i][2]);
    }
    int progressed;
    do {progressed=0;
        for(int i=0;i<P;i++){
            if(finish[i])continue;
            int can=1;for(int r=0;r<R;r++)if(need[i][r]>work[r])can=0;
            if(can){for(int r=0;r<R;r++)work[r]+=alloc[i][r];finish[i]=1;seq[len++]=i;progressed=1;printf("P%d can finish; Work becomes [%d %d %d]\n",i,work[0],work[1],work[2]);}
        }
    } while(progressed);
    if(len==P){printf("SAFE: sequence ");for(int i=0;i<len;i++)printf("%sP%d",i?" -> ":"",seq[i]);puts("");}
    else {printf("UNSAFE: no complete safe sequence; unfinished:");for(int i=0;i<P;i++)if(!finish[i])printf(" P%d",i);puts("");}
    return (unsafe?(len==P):(len!=P))?-1:0;
}
static int worker_demo(int fail){
    printf("\n=== WORKER %s ===\n",fail?"SIMULATED FAILURE":"NORMAL COMPLETION");
    fflush(NULL);pid_t pid=fork();if(pid<0){perror("fork");return -1;}
    if(pid==0){printf("Child worker PID=%ld: %s\n",(long)getpid(),fail?"SIMULATING exit code 42":"work completed");fflush(stdout);_exit(fail?42:0);}
    int status;pid_t result;do{result=waitpid(pid,&status,0);}while(result<0&&errno==EINTR);
    if(result<0){perror("waitpid");return -1;}
    printf("Parent PID=%ld reaped child PID=%ld\n",(long)getpid(),(long)result);
    if(WIFEXITED(status)){printf("Child exited normally with status %d%s\n",WEXITSTATUS(status),fail?" (CONTROLLED SIMULATED FAILURE)":"");return (fail?(WEXITSTATUS(status)!=42):(WEXITSTATUS(status)!=0))?-1:0;}
    if(WIFSIGNALED(status))printf("Child terminated by signal %d\n",WTERMSIG(status));
    return -1;
}
static int limit_demo(void){
    printf("\n=== PROCESS RESOURCE LIMIT: RLIMIT_FSIZE ===\n");
    char tmp[]="/tmp/campuscompute_limit_XXXXXX";int fd=mkstemp(tmp);if(fd<0){perror("mkstemp");return -1;}
    fflush(NULL);pid_t pid=fork();if(pid<0){perror("fork");close(fd);unlink(tmp);return -1;}
    if(pid==0){
        struct rlimit lim={1024,1024};
        if(setrlimit(RLIMIT_FSIZE,&lim)<0){perror("setrlimit");_exit(3);}
        printf("Child: file size soft/hard limit = 1024 bytes\n");fflush(stdout);
        char data[2048];memset(data,'X',sizeof(data));
        /* A write beyond RLIMIT_FSIZE raises SIGXFSZ (default: terminate). */
        /* On Linux, exceeding RLIMIT_FSIZE normally terminates the child with SIGXFSZ. */
        ssize_t nw=write(fd,data,sizeof(data));
        if(nw<0)perror("write failed");
        else {
            fprintf(stderr,"Child: first write returned %zd bytes\n",nw);
            /* If the first write was partial, a further write triggers SIGXFSZ. */
            if ((size_t)nw < sizeof(data)) {
                ssize_t next = write(fd, data + nw, 1);
                if (next < 0) perror("second write failed");
            }
        }
        _exit(4);
    }
    close(fd);int status,ok=0;pid_t got;do{got=waitpid(pid,&status,0);}while(got<0&&errno==EINTR);
    if(got<0)perror("waitpid");
    else if(WIFSIGNALED(status)&&WTERMSIG(status)==SIGXFSZ){puts("Parent: observed SIGXFSZ: file size limit enforced.");ok=1;}
    else if(WIFEXITED(status))printf("Parent: child exited with code %d (expected SIGXFSZ)\n",WEXITSTATUS(status));
    else if(WIFSIGNALED(status))printf("Parent: unexpected signal %d\n",WTERMSIG(status));
    if(unlink(tmp)<0){perror("unlink temporary test file");return -1;}
    puts("Temporary test file removed.");return ok?0:-1;
}
static int cleanup_demo(void){
    printf("\n=== CLEANUP ===\n");
    if(unlink(LOG_FILE)==0)puts("Removed this project's campuscompute.log");
    else if(errno==ENOENT)puts("No project log exists; nothing to remove");
    else {perror("unlink log");return -1;}
    puts("Threads/processes are joined/reaped per demo; named IPC objects unlinked in ipc demo.");
    return 0;
}
static int options(int argc,char **argv,int start,const char **file,int *quantum){
    *file=NULL;*quantum=2;
    for(int i=start;i<argc;i++){
        if(!strcmp(argv[i],"--file")){
            if(*file||i+1>=argc){fprintf(stderr,"--file needs one path and can occur once\n");return -1;}*file=argv[++i];
        } else if(!strcmp(argv[i],"--quantum")){
            if(i+1>=argc||parse_int(argv[++i],1,100000,quantum)){
                fprintf(stderr,"--quantum must be an integer from 1 to 100000\n");return -1;
            }
        } else {fprintf(stderr,"Unknown argument: %s\n",argv[i]);return -1;}
    }
    return 0;
}
static int run_all(void){
    Job jobs[MAX_JOBS];int n;default_jobs(jobs,&n);int fail=0;
    for(int m=0;m<3;m++)if(schedule_demo(jobs,n,m,2,1,NULL))fail++;
    if(compare_demo(jobs,n,2))fail++;
    if(logger_demo())fail++;
    if(ipc_demo())fail++;
    if(banker_demo(0))fail++;
    if(banker_demo(1))fail++;
    if(worker_demo(0))fail++;
    if(worker_demo(1))fail++;
    if(limit_demo())fail++;
    printf("\n=== INTEGRATED SUMMARY ===\nModules: FCFS, RR, Priority, comparison, logger, IPC, Banker's x2, worker x2, resource limit\nFailures: %d; generated log: %s\n",fail,LOG_FILE);
    return fail?1:0;
}
static int interactive_menu(void);
int main(int argc,char **argv){
    if(argc<2){usage(argv[0]);return 2;}
    const char *cmd=argv[1];
    if(!strcmp(cmd,"help")||!strcmp(cmd,"--help")){if(argc!=2)return 2;usage(argv[0]);return 0;}
    if(!strcmp(cmd,"fcfs")||!strcmp(cmd,"rr")||!strcmp(cmd,"priority")||!strcmp(cmd,"compare")){
        const char *file;int quantum;Job j[MAX_JOBS];int n;
        if(options(argc,argv,2,&file,&quantum)){usage(argv[0]);return 2;}
        if(load_jobs(file,j,&n))return 2;
        printf("Loaded %d validated jobs from %s\n",n,file?file:"built-in demo data");
        if(!strcmp(cmd,"compare"))return compare_demo(j,n,quantum)?1:0;
        return schedule_demo(j,n,!strcmp(cmd,"fcfs")?0:!strcmp(cmd,"rr")?1:2,quantum,1,NULL)?1:0;
    }
    if(!strcmp(cmd,"banker")){
        if(argc!=3){fprintf(stderr,"Usage: %s banker safe|unsafe|STATE_FILE\n",argv[0]);return 2;}
        if(!strcmp(argv[2],"safe"))return banker_demo(0)?1:0;
        if(!strcmp(argv[2],"unsafe"))return banker_demo(1)?1:0;
        return banker_custom(argv[2])?1:0;
    }
    if(!strcmp(cmd,"worker")){
        if(argc!=3||(strcmp(argv[2],"normal")&&strcmp(argv[2],"fail"))){fprintf(stderr,"Usage: %s worker normal|fail\n",argv[0]);return 2;}
        return worker_demo(!strcmp(argv[2],"fail"))?1:0;
    }
    if(argc!=2){fprintf(stderr,"This command accepts no extra arguments\n");usage(argv[0]);return 2;}
    if(!strcmp(cmd,"menu"))return interactive_menu();
    if(!strcmp(cmd,"logger"))return logger_demo()?1:0;
    if(!strcmp(cmd,"ipc"))return ipc_demo()?1:0;
    if(!strcmp(cmd,"limit"))return limit_demo()?1:0;
    if(!strcmp(cmd,"cleanup"))return cleanup_demo()?1:0;
    if(!strcmp(cmd,"all"))return run_all();
    fprintf(stderr,"Unknown command: %s\n",cmd);usage(argv[0]);return 2;
}

/* Optional interactive entrypoint; CLI commands remain independently testable. */
static int interactive_menu(void){
    const char *choices[]={"fcfs","rr","priority","compare","logger","ipc","banker","banker","worker","worker","limit","all","cleanup"};
    const char *extra[]={NULL,NULL,NULL,NULL,NULL,NULL,"safe","unsafe","normal","fail",NULL,NULL,NULL};
    puts("\nCampusCompute interactive menu");
    for(;;){
        puts("\n1 FCFS | 2 RR | 3 Priority | 4 Compare | 5 Logger | 6 IPC");
        puts("7 Banker SAFE | 8 Banker UNSAFE | 9 Worker normal | 10 Worker fail");
        puts("11 Resource limit | 12 Integrated demo | 13 Cleanup | 0 Exit");
        printf("Choose 0-13: ");fflush(stdout);
        char line[80];if(!fgets(line,sizeof(line),stdin))return 0;
        char *e;errno=0;long option=strtol(line,&e,10);
        while(*e==' '||*e=='\t')e++;
        if(errno || e==line || (*e!='\n' && *e!='\0') || option<0 || option>13){puts("Invalid input: enter a whole number from 0 to 13.");continue;}
        if(option==0)return 0;
        char *args[4]={"./campuscompute",(char *)choices[option-1],(char *)extra[option-1],NULL};
        int rc=main(extra[option-1]?3:2,args);
        printf("Command finished with exit status %d\n",rc);
    }
}