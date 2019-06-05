//test main

#include "bpt.h"
#include "sys_catalog.h"
#include <assert.h>
#include <unistd.h>
#include <stdint.h>
#include <string>
#include <stdio.h>
#include "join.h"
#include <time.h>
#include <string.h>
#include <iostream>

#define NCOLUMN 5
#define AMAX 20
#define BMAX 30

using namespace std;

int main()
{  
    /*
    init_db(100);
    int tid[3];
    string table1 = "t1";
    string table2 = "t2";
    string table3 = "t3";
    tid[0] = open_table((char*)table1.c_str(), 2);
    tid[1] = open_table((char*)table2.c_str(), 3);
    tid[2] = open_table((char*)table3.c_str(), 2);
    cout << "tid1 = " << tid[0] << " tid2 = " << tid[1] << " tid3 = " << tid[2] << endl;

    int64_t arr1[1];
    int64_t arr2[2];
    int id;
    int64_t key, val1, val2;
    while(scanf("%d", &id) != EOF) {
        if(id == 1 || id == 3) {
            scanf("%lld %lld", &key, &val1);
            arr1[0] = val1;
            if(id == 1) {
                if(insert(tid[0], key, arr1) < 0) {
                    printf("insert failed tid = %d, key = %lld\n", tid[0], key);
                    exit(-1);
                }
            }
            else if(id == 3) {
                if(insert(tid[2], key, arr1) < 0) {
                    printf("insert failed tid = %d, key = %lld\n", tid[2], key);
                    exit(-1);
                }
            }
        }
        else if(id == 2) {
            scanf("%ld %ld %ld", &key, &val1, &val2);
            arr2[0] = val1;
            arr2[1] = val2;
            if(insert(tid[1], key, arr2) < 0) {
                printf("insert failed tid = %d, key = %lld\n", tid[1], key);
                exit(-1);
            }
        }
    }
    close_table(tid[0]);
    close_table(tid[1]);
    close_table(tid[2]);
    shutdown_db();
    */

    init_db(100);
    
    /* small */
    /*
    string small_table1 = "small-01.tab";
    string small_table2 = "small-02.tab";
    string small_table3 = "small-03.tab";
    string small_table4 = "small-04.tab";
    */

    /* medium */
    
    string medium_table1 = "medium-01.tab";
    string medium_table2 = "medium-02.tab";
    string medium_table3 = "medium-03.tab";
    string medium_table4 = "medium-04.tab";
    string medium_table5 = "medium-05.tab";
    string medium_table6 = "medium-06.tab";
    

    /* small open table */
    /*
    open_table((char*)small_table1.c_str(), 5);
    open_table((char*)small_table2.c_str(), 5);
    open_table((char*)small_table3.c_str(), 5);
    open_table((char*)small_table4.c_str(), 5);
    */

    /* medium open table */
    
    open_table((char*)medium_table1.c_str(), 5);
    open_table((char*)medium_table2.c_str(), 5);
    open_table((char*)medium_table3.c_str(), 5);
    open_table((char*)medium_table4.c_str(), 5);
    open_table((char*)medium_table5.c_str(), 5);
    open_table((char*)medium_table6.c_str(), 5);
    
    /* measure time */
    clock_t start, end;
    float t;

    string query;
    int line = -1;
    int64_t answer, my_answer;
    start = clock();
    while(cin >> query) {
        cin >> answer;
        line = line + 2;
        
        cout << "query = " << query << "at line = " << line << endl;
        my_answer = join(query.c_str());
        if(answer != my_answer) {
            cout << "=============== Error ===============" << endl;
            cout << "query = " << query << endl;
            cout << "Answer = " << answer << "My answer = " << my_answer << endl;
            cout << "Line = " << line << endl;
            exit(-1);
        }
        cout << "answer = " << my_answer << endl;
    }
    end = clock();

    t = (float)(end - start) / CLOCKS_PER_SEC;
    cout << "total time = " << t << endl;
    cout << "calculate sum time = " << op_total << endl;
}