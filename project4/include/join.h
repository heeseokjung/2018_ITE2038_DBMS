#ifndef __JOIN__
#define __JOIN__

#include "buffer_manager.h"
#include "sys_catalog.h"
#include <stdint.h>
#include <pthread.h>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <set>
#include <map>

using namespace std;

#define NUM_THREAD 10

typedef struct select_table
{
    vector<vector<int64_t> > tuple;
    int tid;
    int cid;
    uint64_t num_key;
    int is_valid;
}select_table;

typedef struct join_table
{
    select_table left_table;
    select_table right_table;
}join_table;

typedef struct argument
{
    select_table* right;
    vector<vector<int64_t> >* new_tuple;
    unordered_multimap<int64_t, vector<int64_t> >* hash_table;
    int rcid;
    int thread_id;
}argument_t;

typedef struct hash
{
    select_table* left;
    unordered_multimap<int64_t, vector<int64_t> >* hash_table;
    int lcid;
    int thread_id;
}hash_t;

// In-memory table for Join
extern select_table m_table[NUM_TABLES + 1];

// Thread pool
extern pthread_t threads[NUM_THREAD];

// Thread_pool for left table
extern pthread_t left_threads[NUM_THREAD];

// To check new thread init succeed
extern uint64_t* init_finish[NUM_THREAD];

// Lock and condition variable
extern pthread_mutex_t mutex;
extern pthread_mutex_t t_lock;
extern pthread_cond_t cond;
extern pthread_cond_t hash_cond;

// To check DB shutdown
extern bool shutdown_all_thread;

// Argument
extern argument_t argv[NUM_THREAD];

// Hash table(left table)
extern hash_t hash_list[NUM_THREAD];

// Left-table init
extern bool multi_hash_init[NUM_THREAD];

// To check left-table hashing finished
extern int hash_nthread;

// To check thread finished
extern int nthread;

extern bool multi_left;

//debug
extern clock_t op_start;
extern clock_t op_end;
extern float op_total;

/* API */
void pre_scan_table(int tid);
vector<join_table> parse_query(string query);
select_table scan_table(int tid);
vector<vector<join_table>::iterator> query_optimizer(vector<join_table>& ops);
void* multithread_hash_left_table(void* arg);
void* multithread_join_right_table(void* arg);
select_table operate_join(select_table left, select_table right);
int get_cid(const vector<int64_t>& v, int tid, int cid);
int64_t get_result(const select_table* root);
int64_t join (const char* query);

#endif