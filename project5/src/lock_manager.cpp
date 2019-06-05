#include "lock_manager.h"
#include <stdlib.h>
#include <assert.h>
#include <vector>
#include <algorithm>

// Lock table latch
pthread_mutex_t lock_table_latch = PTHREAD_MUTEX_INITIALIZER;

// Lock table
unordered_map<pagenum_t, list<lock_t*> > lock_table;

// Deadlock detector
bool deadlock_detect(int conflict_txn, vector<int>& v);

lock_t* make_lock_object(int tid, int txid, int mode, pagenum_t pgid)
{
    lock_t* new_lock = new lock_t;
    new_lock->table_id = tid;
    new_lock->page_id = pgid;
    new_lock->txid = txid;
    new_lock->lock_mode = mode;
    pthread_mutex_lock(&txn_mutex);
    auto tx = txn_table.find(txid);
    new_lock->txn = tx->second;
    tx->second->txn_locks.push_back(new_lock);
    tx->second->access_page.insert(make_pair(pgid, tid));
    pthread_mutex_unlock(&txn_mutex);
    return new_lock;
}

int acquire_lock(int table_id, int txid, int mode, pagenum_t page_id)
{
    pthread_mutex_lock(&lock_table_latch);
    
    auto lock_bucket = lock_table.find(page_id);
    // Other lock doesn't exist
    if(lock_bucket == lock_table.end() || lock_bucket->second.empty()) {
        lock_t* lock_obj = make_lock_object(table_id, txid, mode, page_id);
        lock_obj->lock_state = LOCK_GRANTED;
        list<lock_t*> lock_list;
        lock_list.push_back(lock_obj);
        lock_table.insert(make_pair(page_id, lock_list));
        pthread_mutex_unlock(&lock_table_latch);
        return LOCK_GRANTED;
    }
    // Other lock already exist
    else {
        unsigned int is_compatible = 1;
        for(auto it = lock_bucket->second.begin(); it != lock_bucket->second.end(); ++it) {
            if((*it)->table_id == table_id) {
                // Request to same object
                if((*it)->txid == txid) {
                    int state = (*it)->lock_state;
                    pthread_mutex_unlock(&lock_table_latch);
                    return state;
                }
                else if((*it)->txid != txid)
                    is_compatible = (*it)->lock_mode & mode;
            }
        }

        // No conflict
        if(is_compatible) {
            lock_t* lock_obj = make_lock_object(table_id, txid, mode, page_id);
            lock_obj->lock_state = LOCK_GRANTED;
            lock_bucket->second.push_back(lock_obj);
            pthread_mutex_unlock(&lock_table_latch);
            return LOCK_GRANTED;
        }
        // Conflict
        else {
            //first check deadlock
            vector<int> v;
            for(auto it = lock_bucket->second.begin(); it != lock_bucket->second.end(); ++it) {
                if((*it)->table_id == table_id && (*it)->txid != txid)
                    v.push_back((*it)->txid);
            }

            bool is_deadlock = deadlock_detect(txid, v);
            if(is_deadlock) {
                pthread_mutex_unlock(&lock_table_latch);
                return LOCK_DEADLOCK;
            }
            
            // In case no deadlock, wait
            else {
                lock_t* lock_obj = make_lock_object(table_id, txid, mode, page_id);
                lock_obj->lock_state = LOCK_WAITING;
                pthread_mutex_lock(&txn_mutex);
                auto tx = txn_table.find(txid);
                tx->second->txn_state = WAITING;
                for(auto it = lock_bucket->second.begin(); it != lock_bucket->second.end(); ++it) {
                    if((*it)->table_id = table_id && (*it)->txid != txid) {
                        if(find(tx->second->wait_lock.begin(), tx->second->wait_lock.end(), *it) == tx->second->wait_lock.end())
                            tx->second->wait_lock.push_back(*it);
                    }
                }
                pthread_mutex_unlock(&txn_mutex);
                lock_bucket->second.push_back(lock_obj);
                pthread_mutex_unlock(&lock_table_latch);
                return LOCK_WAITING;
            }
        }
    }
}

void release_lock(int txid)
{
    set<pair<pagenum_t, pagenum_t> > page;
    pthread_mutex_lock(&txn_mutex);
    auto tx = txn_table.find(txid);
    for(auto it = tx->second->access_page.begin(); it != tx->second->access_page.end(); ++it)
        page.insert(*it);
    pthread_mutex_unlock(&txn_mutex);

    pthread_mutex_lock(&lock_table_latch);
    for(auto p = page.begin(); p != page.end(); ++p) {
        auto lock_bucket = lock_table.find(p->first);
        auto it = lock_bucket->second.begin();
        // Erase lock objects
        while(it != lock_bucket->second.end()) {
            if((*it)->txid == txid) {
                free(*it);
                it = lock_bucket->second.erase(it);
            }
            else
                it++;
        }

        // Change the next txn's state
        auto t = lock_bucket->second.begin();
        if(t == lock_bucket->second.end())
            continue;
        int first_txid = (*t)->txid;
        while(t != lock_bucket->second.end()) {
            if((*t)->txid == first_txid) {
                (*t)->lock_state = LOCK_GRANTED;
                pthread_mutex_lock(&txn_mutex);
                auto txn = txn_table.find((*t)->txid);
                txn->second->txn_state = GRANTED;
                pthread_cond_signal(&txn->second->txn_cond);
                pthread_mutex_unlock(&txn_mutex);
            }
            t++;
        }
    }
    pthread_mutex_unlock(&lock_table_latch);
}