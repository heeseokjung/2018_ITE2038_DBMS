#include "txn_manager.h"
#include <set>
#include <stdlib.h>
#include <assert.h>

// Transaction table
unordered_map<int, txn_t*> txn_table;

// Transaction pool
bool txn_pool[MAX_TXID + 1];

// Alloc txid
int alloc = 0;

// Aborted transaction
set<int> aborted_txn;

// Transaction table mutex
pthread_mutex_t txn_mutex = PTHREAD_MUTEX_INITIALIZER;

void release_lock(int txid);

static int alloc_txid()
{
    while(true) {
        alloc = ++alloc % (MAX_TXID + 1);
        if(alloc == 0)
            alloc++;
        if(txn_pool[alloc] == false) {
            txn_pool[alloc] = true;
            return alloc;
        }
    }
}

void txn_abort(int txid, int* result)
{
    *result = 0;
    set<pair<pagenum_t, pagenum_t> > page;
    pthread_mutex_lock(&txn_mutex);
    auto tx = txn_table.find(txid);
    for(auto it = tx->second->access_page.begin(); it != tx->second->access_page.end(); ++it)
        page.insert(*it);
    pthread_mutex_unlock(&txn_mutex);

    pthread_mutex_lock(&bufcache_latch);
    for(auto it = page.begin(); it != page.end(); ++it) {
        buf_frame* buf = buf_get_page(it->second, it->first);
        buf->is_dirty = 0;
        buf_put_page(buf, 0);
    }
    pthread_mutex_unlock(&bufcache_latch);

    release_lock(txid);

    pthread_mutex_lock(&txn_mutex);
    aborted_txn.insert(txid);
    pthread_mutex_unlock(&txn_mutex);
}

void init_txn_manager()
{
    pthread_mutex_lock(&txn_mutex);
    for(int i = 1; i <= MAX_TXID; ++i)
        txn_pool[i] = false;
    pthread_mutex_unlock(&txn_mutex);
}

int begin_tx()
{
    pthread_mutex_lock(&txn_mutex);
    int txid = alloc_txid();
    pthread_mutex_unlock(&txn_mutex);

    txn_t* tx = new txn_t;
    tx->txid = txid;
    tx->txn_state = IDLE;
    tx->txn_cond = PTHREAD_COND_INITIALIZER;
    pthread_mutex_lock(&txn_mutex);
    txn_table.insert(make_pair(txid, tx));
    pthread_mutex_unlock(&txn_mutex);

    return txid;
}

int end_tx(int txid)
{
    pthread_mutex_lock(&txn_mutex);
    // Aborted transaction
    if(aborted_txn.find(txid) != aborted_txn.end()) {
        aborted_txn.erase(txid);
        txn_pool[txid] = false;
        auto tx = txn_table.find(txid);
        free(tx->second);
        txn_table.erase(tx);
        pthread_mutex_unlock(&txn_mutex);
        return 0;
    }
    pthread_mutex_unlock(&txn_mutex);
    
    release_lock(txid);

    pthread_mutex_lock(&txn_mutex);
    auto tx = txn_table.find(txid);
    txn_pool[txid] = false;
    free(tx->second);
    txn_table.erase(tx);
    pthread_mutex_unlock(&txn_mutex);
    return txid;
}