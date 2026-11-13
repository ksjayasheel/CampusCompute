/* Custom Banker safety analysis: actual input matrices, never canned answers. */
#include <stdio.h>
#include <stdlib.h>
#define MAXP 32
#define MAXR 16
int banker_custom(const char *path) {
    FILE *f=fopen(path,"r");
    if(!f){perror("open Banker state");return -1;}
    int p,r,avail[MAXR],alloc[MAXP][MAXR],max[MAXP][MAXR],need[MAXP][MAXR];
    if(fscanf(f,"%d %d",&p,&r)!=2 || p<1||p>MAXP||r<1||r>MAXR){fprintf(stderr,"Invalid Banker dimensions (1..32 processes, 1..16 resources)\n");fclose(f);return -1;}
    for(int j=0;j<r;j++)if(fscanf(f,"%d",&avail[j])!=1||avail[j]<0){fprintf(stderr,"Invalid Available[%d]\n",j);fclose(f);return -1;}
    for(int i=0;i<p;i++)for(int j=0;j<r;j++)if(fscanf(f,"%d",&alloc[i][j])!=1||alloc[i][j]<0){fprintf(stderr,"Invalid Allocation[%d][%d]\n",i,j);fclose(f);return -1;}
    for(int i=0;i<p;i++)for(int j=0;j<r;j++){
        if(fscanf(f,"%d",&max[i][j])!=1||max[i][j]<alloc[i][j]){fprintf(stderr,"Invalid Maximum[%d][%d] (must be >= Allocation)\n",i,j);fclose(f);return -1;}
        need[i][j]=max[i][j]-alloc[i][j];
    }
    char extra[2];if(fscanf(f," %1s",extra)==1){fprintf(stderr,"Unexpected trailing data in Banker input\n");fclose(f);return -1;}
    fclose(f);
    printf("\n=== CUSTOM BANKER STATE: %s ===\n",path);
    printf("Processes=%d Resource types=%d\nAvailable:",p,r);for(int j=0;j<r;j++)printf(" %d",avail[j]);puts("");
    puts("PID | Allocation | Maximum | Need");
    for(int i=0;i<p;i++){
        printf("P%-2d |",i);for(int j=0;j<r;j++)printf(" %d",alloc[i][j]);
        printf(" | ");for(int j=0;j<r;j++)printf("%d ",max[i][j]);
        printf("| ");for(int j=0;j<r;j++)printf("%d ",need[i][j]);puts("");
    }
    int work[MAXR],done[MAXP]={0},sequence[MAXP],length=0;
    for(int j=0;j<r;j++)work[j]=avail[j];
    int progress;
    do {
        progress=0;
        for(int i=0;i<p;i++){
            if(done[i])continue;
            int can=1;for(int j=0;j<r;j++)if(need[i][j]>work[j])can=0;
            if(can){done[i]=1;sequence[length++]=i;progress=1;for(int j=0;j<r;j++)work[j]+=alloc[i][j];
                printf("P%d finishes; Work:",i);for(int j=0;j<r;j++)printf(" %d",work[j]);puts("");
            }
        }
    } while(progress);
    if(length==p){printf("RESULT: SAFE. Sequence: ");for(int i=0;i<length;i++)printf("%sP%d",i?" -> ":"",sequence[i]);puts("");}
    else {printf("RESULT: UNSAFE. Cannot complete:");for(int i=0;i<p;i++)if(!done[i])printf(" P%d",i);puts("");}
    /* Unsafe is a valid algorithm outcome; return success for a valid input file. */
    return 0;
}
