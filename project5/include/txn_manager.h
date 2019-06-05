#ifndef __TXN_MANGER_H__
#define __TXN_MANGER_H__

#include "buffer_manager.h"
#include <stdint.h>
#include <pthread.h>
#include <set>
#include <list>
#include <vector>
#include <unordered_map>

using namespace std;

// hash key
typedef uint64_t pagenum_t;
typedef uint64_t pgoffset_t;

/* Transaction state */
#define IDLE -1
#define GRANTED 0
#define WAITING 1

#define MAX_TXID 100

struct lock_t;
struct txn_t
{
    int txid;
    int txn_state;
    list<struct lock_t*> txn_locks;
    set<pair<pagenum_t, pagenum_t> > access_page;
    vector<struct lock_t*> wait_lock;
    pthread_cond_t txn_cond;
};
typedef struct txn_t txn_t;

// Aborted transaction
extern set<int> aborted_txn;

// Transaction table
extern unordered_map<int, txn_t*> txn_table;

// Transaction pool
extern bool txn_pool[MAX_TXID + 1];

// Alloc txid
extern int alloc;

// Transaction table mutex
extern pthread_mutex_t txn_mutex;

// API
void init_txn_manager();
void txn_abort(int txid, int* result);
int begin_tx();
int end_tx(int txid);

#endif