#ifndef __LOCK_MANAGER_H__
#define __LOCK_MANAGER_H__

#include "txn_manager.h"
#include "buffer_manager.h"
#include <set>
#include <unordered_map>
#include <list>
#include <pthread.h>

using namespace std;

#define MORE_FREE_LOCKS 10

/* Lock mode */
#define S_LOCK 1
#define X_LOCK 0

/* Lock state */
#define LOCK_GRANTED 0   
#define LOCK_WAITING 1
#define LOCK_DEADLOCK -1

struct txn_t;
struct lock_t
{
    int table_id;
    pagenum_t page_id;
    int txid;
    int lock_mode;
    int lock_state;
    struct txn_t* txn;
};
typedef struct lock_t lock_t;

// Lock table latch
extern pthread_mutex_t lock_table_latch;

// Lock table
extern unordered_map<pagenum_t, list<lock_t*> > lock_table;

// API
void init_lock_manager(int num_lock);
lock_t* make_lock_object(int tid, int txid, int mode, pagenum_t pgid);
int acquire_lock(int table_id, int txid, int mode, pagenum_t page_id);
void release_lock(int txid);

#endif