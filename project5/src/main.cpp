//test main

#include "bpt.h"
#include "sys_catalog.h"
#include <assert.h>
#include <unistd.h>
#include <stdint.h>
#include <string>
#include <stdio.h>
#include <txn_manager.h>
#include <lock_manager.h>
#include "join.h"
#include <time.h>
#include <string.h>
#include <iostream>

#define NCOLUMN 5
#define AMAX 20
#define BMAX 30

using namespace std;

static int count = 0;

pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;

pthread_mutex_t k = PTHREAD_MUTEX_INITIALIZER;

void* thread1(void* arg)
{
    int* tid = (int*)arg;
    int txid = begin_tx();
    cout << "Thread1 TXID = " << txid << endl;
    int64_t val[3];
    val[0] = 8;
    val[1] = 9;
    val[2] = 10;
    
    int result;
    update(*tid, 5, val, txid, &result);
    if(result == 0)
        printf("thread 1 aborted\n");
    printf("thread1 updated\n");

    sleep(15);

    cout << "end TXID = " << txid << " ";
    int end = end_tx(txid);
    cout << "return = " << end << endl;
}

void* thread2(void* arg)
{
    int* tid = (int*)arg;
    int txid = begin_tx();
    cout << "Thread2 TXID = " << txid << endl;
    int64_t val[3];
    val[0] = 4;
    val[1] = 5;
    val[2] = 6;
    
    int result;
    int64_t* ret = find(*tid, 5, txid, &result);
    if(result == 0)
        printf("thread 2 aborted\n");
    printf("thread2 read > ");
    for(int i = 0; i < 3; ++i)
        printf("%ld ", ret[i]);
    printf("\n");

    cout << "end TXID = " << txid << " ";
    int end = end_tx(txid);
    cout << "return = " << end << endl;
}

void* thread3(void* arg)
{
    int* tid = (int*)arg;
    int txid = begin_tx();
    cout << "Thread3 TXID = " << txid << endl;
    int64_t val[3];
    val[0] = 4;
    val[1] = 5;
    val[2] = 6;
    for(int i = 100; i <= 520; ++i) {
        int64_t* ret;
        int result;
        if(update(*tid, i, val, txid, &result) < 0)
            printf("Cannot find : txid=%d, key=%d\n", txid, i);
        else {
            if(result == 0) {
                printf("thread3 aborted\n");
                continue;
            }
            
            cout << "Thread3 key = " << i << " >> ";
            
            cout << "update to 4 5 6 ";
            cout << "result = " << result << endl;
        }
        //delay
        //for(int i = 0; i < 10000000; ++i);
    }
    cout << "end TXID = " << txid << " ";
    int end = end_tx(txid);
    cout << "return = " << end << endl;
}

int main()
{  
    init_db(100000);
    string table_name = "test.db";
    int tid = open_table((char*)table_name.c_str(), 4);
    cout << "table ID = " << tid << endl;
  
    int64_t val[3];
    val[0] = 10;
    val[1] = 20;
    val[2] = 30;
    
    pthread_t tx1, tx2, tx3;
    pthread_create(&tx1, 0, thread1, &tid);
    //delay
    for(int i = 0; i < 100000000; ++i);
    for(int i = 0; i < 100000000; ++i);
    for(int i = 0; i < 100000000; ++i);
    pthread_create(&tx2, 0, thread2, &tid);
    //pthread_create(&tx3, 0, thread3, &tid);

    //pthread_yield();

    pthread_join(tx1, NULL);
    pthread_join(tx2, NULL);
    //pthread_join(tx3, NULL);
}