#include "deadlock.h"
#include "lock_manager.h"
#include "txn_manager.h"
#include <vector>
#include <utility>

using namespace std;

static void dfs(int src, vector<int>* adj, bool* visit, bool* black, bool* is_cycle)
{
    visit[src] = true;
    for(int i = 0; i < adj[src].size(); ++i) {
        if(!visit[adj[src][i]])
            dfs(adj[src][i], adj, visit, black, is_cycle);
        else if(!black[adj[src][i]])
            *is_cycle = true;
    }
    black[src] = true;
}

bool deadlock_detect(int conflict_txn, vector<int>& v)
{
    pthread_mutex_lock(&txn_mutex);
    vector<int> adj[MAX_TXID + 1];
    vector<int> vertex;
    for(int i = 0; i < v.size(); ++i)
        adj[conflict_txn].push_back(v[i]);

    for(int i = 1; i <= MAX_TXID; ++i) {
        if(txn_pool[i]) {
            vertex.push_back(i);
            auto tx = txn_table.find(i);
            for(int j = 0; j < tx->second->wait_lock.size(); ++j) {
                int txid = tx->second->wait_lock[j]->txid;
                adj[i].push_back(txid);
            }
        }
    }

    //debug
    /*
    printf("===== wait-for graph =====\n");
    for(auto it = vertex.begin(); it != vertex.end(); ++it) {
        for(auto its = adj[*it].begin(); its != adj[*it].end(); ++its)
            printf("%d -> %d\n", *it, *its);
    }
    printf("==========================\n");
    */
    
    bool visit[MAX_TXID + 1];
    bool black[MAX_TXID + 1];
    for(int i = 1; i <= MAX_TXID; ++i) {
        visit[i] = false;
        black[i] = false;
    }
    bool is_cycle = false;
    for(int i = 0; i < vertex.size(); ++i) {
        dfs(vertex[i], adj, visit, black, &is_cycle);
        if(is_cycle) {
            pthread_mutex_unlock(&txn_mutex);
            return true;
        }
    }
    pthread_mutex_unlock(&txn_mutex);
    return false;
}