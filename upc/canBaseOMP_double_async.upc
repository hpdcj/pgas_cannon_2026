#include <upc_relaxed.h>
#include <stdio.h>
#include <math.h>
#include <time.h>
#include <omp.h>

//upcc canBaseOMP_double_async.upc -o canBaseOMP_double_async.out --network=udp -Wp,"-DN=3600 -DblC=2" -Wc,"-fopenmp -Wall -O2" -Wl,"-fopenmp -Wall -O2"

#define STR_HELPER(x) #x
#define STR(x) STR_HELPER(x)

#define MATRIX_DIR_PATH "/home/pionier/testUPC/" STR(N) "/" STR(blC)
#define MAX_FILENAME_SIZE 125

#define n (N / blC)
#define nn (n * n)

strict shared double shared_A[nn][THREADS];
strict shared double shared_B[nn][THREADS];

void readM(char *filename, double *Dest);
void saveM(char *filename, double *Dest);

void moveBefore(int myI, int myJ, double *A, double *B);

void cannon(int myI, int myJ, double *A, double *B, double *C);

int main(int argc, char *argv[])
{
    struct timespec start, stop;
    long long ansTime;

    struct timespec global_start;
    clock_gettime(CLOCK_MONOTONIC, &global_start);

    double *A;  //podmacierz A
    double *B;  //podmacierz B
    double *C;  //podmacierz wynikowa

    int myI, myJ;

    char filename[MAX_FILENAME_SIZE];

    myI = MYTHREAD / blC;
    myJ = MYTHREAD % blC;

    //------------------------------//
    //DEKLARACJA DYNAMICZNYCH TABLIC//
    //------------------------------//
    A = (double *)malloc(nn * sizeof(double));
    B = (double *)malloc(nn * sizeof(double));
    C = (double *)calloc(nn, sizeof(double));
    upc_barrier;

    //--------------------------------------//
    //         Czytanie danych              //
    //--------------------------------------//
    snprintf(filename, sizeof(filename), "%s%s%d.txt", MATRIX_DIR_PATH, "/A", MYTHREAD);
    readM(filename, A);

    snprintf(filename, sizeof(filename), "%s%s%d.txt", MATRIX_DIR_PATH, "/B", MYTHREAD);
    readM(filename, B);

    upc_barrier;
    moveBefore(myI, myJ, A, B);

    upc_barrier;
    clock_gettime(CLOCK_MONOTONIC, &start);

    #pragma omp parallel
    #pragma omp master
    cannon(myI, myJ, A, B, C);

    clock_gettime(CLOCK_MONOTONIC, &stop);
    ansTime = (stop.tv_sec - start.tv_sec) * 1000000000LL + (stop.tv_nsec - start.tv_nsec);
    printf("[%d] TIME %lf\n", MYTHREAD, ansTime/1e9);

    upc_barrier;
    struct timespec write_start;
    clock_gettime(CLOCK_MONOTONIC, &write_start);

    //Zapisywanie

    snprintf(filename, sizeof(filename), "%s%s%d.txt", MATRIX_DIR_PATH, "/ans/C", MYTHREAD);
    saveM(filename, C);

    //Wyniki

    if (MYTHREAD == 0)
    {
        struct timespec global_stop;
        clock_gettime(CLOCK_MONOTONIC, &global_stop);
        long long readTime = (start.tv_sec - global_start.tv_sec) * 1000000000LL + (start.tv_nsec - global_start.tv_nsec);
        long long multiTime = (write_start.tv_sec - start.tv_sec) * 1000000000LL + (write_start.tv_nsec - start.tv_nsec);
        long long writeTime = (global_stop.tv_sec - write_start.tv_sec) * 1000000000LL + (global_stop.tv_nsec - write_start.tv_nsec);
        long long totalTime = (global_stop.tv_sec - global_start.tv_sec) * 1000000000LL + (global_stop.tv_nsec - global_start.tv_nsec);
        printf("Total:\t%d\t%d\t%d\t%lf\t%lf\t%lf\t%lf\n", N, THREADS, omp_get_max_threads(), readTime/1e9, multiTime/1e9, writeTime/1e9, totalTime/1e9);
    }

    //------------------------------//
    //ZWALNIANIE DYNAMICZNYCH TABLIC//
    //------------------------------//
    upc_barrier;

    free(A);
    free(B);
    free(C);
upc_all_free((shared void*)shared_A);
upc_all_free((shared void*)shared_B);

    return 0;
}
//----------------------------------------//
//----------------------------------------//

void readM(char *filename, double *Dest)
{
    FILE *f;

    if((f = fopen(filename, "r")) == NULL)
    {
        printf("Cannot open file for reading: %s!\n", filename);
        exit(2);
    }

    for(int i = 0; i < n * n; i++)
    {
        if(fscanf(f, "%lf", &Dest[i]) != 1)
        {
            printf("Unable to read matrix: %s\n", filename);
            exit(2);
        }
    }
    fclose(f);
}

void saveM(char *filename, double *Dest)
{
    FILE *f;
    int s = n * n;

    if((f = fopen(filename, "w")) == NULL)
    {
        printf("Cannot open file for writing: %s!\n", filename);
        exit(2);
    }

    int j = 0;
    for(int i = 0; i < s; i++, j++)
    {
        if(j == n)
        {
            fprintf(f, "\n");
            j = 0;
        }
        fprintf(f, "%lf ", Dest[i]);
    }
    fclose(f);
}

//----------------------------------------//

void moveBefore(int myI, int myJ, double *A, double *B)
{
    int k = (myJ + myI) % blC;
    int destA = myI * blC + k;
    int destB = k * blC + myJ;

    upc_barrier;
    //Wysyłam
    upc_memput((shared void*)&shared_A[0][destA], (void*)A, sizeof(double) * nn);
    upc_memput((shared void*)&shared_B[0][destB], (void*)B, sizeof(double) * nn);

    upc_barrier;

    //Odbieram
    upc_memget((void*)A, (shared void*)&shared_A[0][MYTHREAD], sizeof(double) * nn);
    upc_memget((void*)B, (shared void*)&shared_B[0][MYTHREAD], sizeof(double) * nn);
}

//----------------------------------------//

void multiplyOmpTask(int i, double *A, double *B, double *C)
{
    for(int y = 0; y < n; y++)
    {
        long Aiy = A[i*n+y];
        for(int j = 0; j < n; j++) 
        {
            C[i*n+j] += Aiy * B[y*n+j];
        }
    }
}

void multiplyOmp(double *A, double *B, double *C)
{
    for(int i = 0; i < n; i++) 
    {
        #pragma omp task firstprivate(i) shared(C)
        multiplyOmpTask(i, A, B, C);
    }
    #pragma omp taskwait
}

void cannon(int myI, int myJ, double *A, double *B, double *C)
{
    int destA = myI * blC + ((myJ - 1 + blC) % blC);
    int destB = ((myI - 1 + blC) % blC) * blC + myJ;
    for(int p = 1; p <= blC; p++)
    {
        if (p < blC)
        {
            upc_memput_nbi((shared void*)&shared_A[0][destA], (void*)A, sizeof(double) * nn);
            upc_memput_nbi((shared void*)&shared_B[0][destB], (void*)B, sizeof(double) * nn);
        }

        multiplyOmp(A, B, C);

        if (p < blC)
        {
            upc_synci();
            upc_memget_nbi((void*)A, (shared void*)&shared_A[0][MYTHREAD], sizeof(double) * nn);
            upc_memget_nbi((void*)B, (shared void*)&shared_B[0][MYTHREAD], sizeof(double) * nn);
            upc_synci();
        }
        upc_barrier;
    }
}
