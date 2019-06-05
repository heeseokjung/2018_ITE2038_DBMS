#include "join.h"
#include <assert.h>
#include <stdlib.h>
#include <algorithm>
#include <iostream>
#include <time.h>

// In-memory tables for Join
select_table m_table[NUM_TABLES + 1];

// Thread pool
pthread_t threads[NUM_THREAD];

// Thread_pool for left table
pthread_t left_threads[NUM_THREAD];

// Lock and condition variable
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t t_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
pthread_cond_t hash_cond = PTHREAD_COND_INITIALIZER;

// To check new thread init succeed
uint64_t* init_finish[NUM_THREAD];

// To check thread finished
int nthread = 0;

// To check DB shutdown
bool shutdown_all_thread = false;

// Argument
argument_t argv[NUM_THREAD];

// Hash table(left table)
hash_t hash_list[NUM_THREAD];

// Left-table init
bool multi_hash_init[NUM_THREAD];

// To check left-table hashing finished
int hash_nthread = 0;

bool multi_left = false;

//debug
clock_t op_start = 0;
clock_t op_end = 0;
float op_total = 0;

vector<join_table> parse_query(string query)
{
    vector<join_table> ret;
    int ltid, lcid, rtid, rcid;
    char dot, equal, empersand;
    istringstream iss(query);
    
    while(true)
    {
        iss >> ltid >> dot >> lcid;
        assert(dot == '.');
        iss >> equal;
        assert(equal == '=');
        iss >> rtid >> dot >> rcid;
        assert(dot == '.');
        iss >> empersand;
        join_table op;
        op.left_table.tid = m_table[ltid].tid;
        op.left_table.num_key = m_table[ltid].num_key;
        op.left_table.cid = lcid;
        op.left_table.is_valid = 0;
        op.right_table.tid = m_table[rtid].tid;
        op.right_table.num_key = m_table[rtid].num_key;
        op.right_table.cid = rcid;
        op.right_table.is_valid = 0;
        ret.push_back(op);
        if(!empersand || empersand != '&')
            break;
        empersand = '\0';
    }
    return ret;
}

void pre_scan_table(int tid)
{
    select_table scan;
    header = buf_get_page(tid, 0);
    pagenum_t root = PGNUM(header->frame->rootpg_offset);
    buf_put_page(header, 0);
    if(root == 0) {
        scan.tid = tid;
        scan.num_key = 0;
        m_table[tid] = scan;
        return;
    }

    // Find left-most leaf node
    buf_frame* buf_node = buf_get_page(tid, root);
    pagenum_t next_pgnum = buf_node->page_num;
    while(!buf_node->frame->is_leaf) {
        next_pgnum = PGNUM(buf_node->frame->far_left);
        buf_put_page(buf_node, 0);
        buf_node = buf_get_page(tid, next_pgnum);
    }

    // Traverse leaf nodes
    //debug
    int min_key = buf_node->frame->record[0].key;
    int max_key = buf_node->frame->record[0].key;
    uint64_t num_key = 0;
    buf_put_page(buf_node, 0);
    while(next_pgnum != 0) {
        buf_node = buf_get_page(tid, next_pgnum);
        for(int i = 0; i < buf_node->frame->num_keys; ++i) {
            vector<int64_t> v;
            v.push_back((int64_t)tid);
            v.push_back(buf_node->frame->record[i].key);
            for(int j = 0; j < cat_pointer[tid]->num_column - 1; ++j)
                v.push_back(buf_node->frame->record[i].values[j]);
            if(buf_node->frame->record[i].key < min_key)
                min_key = buf_node->frame->record[i].key;
            else if(buf_node->frame->record[i].key > max_key)
                max_key = buf_node->frame->record[i].key;
            num_key++;
            scan.tuple.push_back(v);
        }
        next_pgnum = PGNUM(buf_node->frame->sibling);
        buf_put_page(buf_node, 0);
    }
    scan.tid = tid;
    scan.num_key = num_key;
    m_table[tid] = scan;
    cat_pointer[tid]->min_key = min_key;
    cat_pointer[tid]->max_key = max_key;
    cat_pointer[tid]->num_key = num_key;
}

vector<vector<join_table>::iterator> query_optimizer(vector<join_table>& ops)
{
    vector<vector<join_table>::iterator> optimized_ops;
    multimap<uint64_t, vector<join_table>::iterator> sorted_nkey;
    set<int> s; //tid
    
    for(auto it = ops.begin(); it != ops.end(); ++it) {
        uint64_t sum_nkey = it->left_table.num_key + it->right_table.num_key;
        sorted_nkey.insert(make_pair(sum_nkey, it));
    }

    // First join set
    auto it = sorted_nkey.begin();
    optimized_ops.push_back(it->second);
    s.insert(it->second->left_table.tid);
    s.insert(it->second->right_table.tid);
    sorted_nkey.erase(it);

    // Rest tables
    while(!sorted_nkey.empty()) {
        auto it = sorted_nkey.begin();
        while(it != sorted_nkey.end()) {
            if(s.find(it->second->left_table.tid) != s.end() || 
               s.find(it->second->right_table.tid) != s.end()) {
                optimized_ops.push_back(it->second);
                if(s.find(it->second->left_table.tid) == s.end()) {
                    select_table tmp;
                    tmp = it->second->left_table;
                    it->second->left_table = it->second->right_table;
                    it->second->right_table = tmp;
                }
                s.insert(it->second->left_table.tid);
                s.insert(it->second->right_table.tid);
                sorted_nkey.erase(it++);
            }
            else
                it++;
        }
    }

    return optimized_ops;
}

// Concatenate two vectors
vector<int64_t> operator+ (const vector<int64_t>& v1, const vector<int64_t>& v2)
{
    vector<int64_t> concat;
    concat.reserve(v1.size() + v2.size());
    concat.insert(concat.end(), v1.begin(), v1.end());
    concat.insert(concat.end(), v2.begin(), v2.end());
    return concat;
}

void* multithread_hash_left_table(void* arg)
{
    hash_t* a = (hash_t*)arg;
    int thread_id = a->thread_id;

    pthread_mutex_lock(&mutex);
    multi_hash_init[thread_id] = true;
    pthread_cond_wait(&hash_cond, &mutex);
    pthread_mutex_unlock(&mutex);

    while(!shutdown_all_thread) {
        select_table* left = a->left;
        unordered_multimap<int64_t, vector<int64_t> > hash_table;
        int lcid = a->lcid;

        uint64_t start = (left->num_key / NUM_THREAD) * thread_id;
        uint64_t end = (left->num_key / NUM_THREAD) * (thread_id + 1);
        if(thread_id == NUM_THREAD - 1)
            end = left->num_key;

        for(uint64_t i = start; i < end; ++i)
            hash_table.insert(make_pair(left->tuple[i][lcid], left->tuple[i]));

        pthread_mutex_lock(&mutex);
        a->hash_table = &hash_table;
        hash_nthread++;
        pthread_cond_wait(&hash_cond, &mutex);
        pthread_mutex_unlock(&mutex);
    }
}

void* multithread_join_right_table(void* arg)
{
    argument_t* a = (argument_t*)arg;
    int thread_id = a->thread_id;
    uint64_t local_num_key = 0;

    pthread_mutex_lock(&mutex);
    init_finish[thread_id] = &local_num_key;
    pthread_cond_wait(&cond, &mutex);
    pthread_mutex_unlock(&mutex);

    while(!shutdown_all_thread) {
        select_table* right = a->right;
        unordered_multimap<int64_t, vector<int64_t> >* hash_table = a->hash_table;
        vector<vector<int64_t> > new_tuple;
        int rcid = a->rcid;

        uint64_t start = (right->num_key / NUM_THREAD) * thread_id;
        uint64_t end = (right->num_key / NUM_THREAD) * (thread_id + 1);
        if(thread_id == NUM_THREAD - 1)
            end = right->num_key;

        for(uint64_t i = start; i < end; ++i) {
            if(multi_left) {
                for(int j = 0; j < NUM_THREAD; ++j) {
                    auto its = hash_list[j].hash_table->equal_range(right->tuple[i][rcid]);
                    for(auto bucket_it = its.first; bucket_it != its.second; ++bucket_it) {
                        vector<int64_t> tmp = bucket_it->second + right->tuple[i];
                        new_tuple.push_back(move(tmp));
                        local_num_key++;
                    }
                }
            }
            else {
                auto its = hash_table->equal_range(right->tuple[i][rcid]);
                for(auto bucket_it = its.first; bucket_it != its.second; ++bucket_it) {
                    vector<int64_t> tmp = bucket_it->second + right->tuple[i];
                    new_tuple.push_back(move(tmp));
                    local_num_key++;
                }
            }
        }

        pthread_mutex_lock(&mutex);
        a->new_tuple = &new_tuple;
        nthread++;
        pthread_cond_wait(&cond, &mutex);
        pthread_mutex_unlock(&mutex);
    }

    return NULL;
}

select_table operate_join(select_table* left, select_table* right)
{
    select_table ret_table;
    unordered_multimap<int64_t, vector<int64_t> > hash_table;
    uint64_t new_num_key = 0;
    multi_left = false;

    // Build hash table with left table
    int lcid;
    if(!left->is_valid) {
        lcid = get_cid(m_table[left->tid].tuple[0], left->tid, left->cid); 
        left = &m_table[left->tid];
    }
    else
        lcid = get_cid(left->tuple[0], left->tid, left->cid);

    //multi thread ver
    if(100 < left->num_key) { 
        hash_nthread = 0;
        for(int i = 0; i < NUM_THREAD; ++i) {
            hash_list[i].left = left;
            hash_list[i].lcid = lcid;
        }

        pthread_mutex_lock(&mutex);
        pthread_cond_broadcast(&hash_cond);
        pthread_mutex_unlock(&mutex);

        while(true) {
            if(hash_nthread == NUM_THREAD)
                break;
            pthread_yield();
        }
        multi_left = true;
    }
    else {
        for(int i = 0; i < left->tuple.size(); ++i)
            hash_table.insert(make_pair(left->tuple[i][lcid], left->tuple[i]));
    }

    // Join right table
    int rcid;
    if(!right->is_valid) {
        rcid = get_cid(m_table[right->tid].tuple[0], right->tid, right->cid); 
        right = &m_table[right->tid];
    }
    else
        rcid = get_cid(right->tuple[0], right->tid, right->cid);
    
    if(100 < right->num_key) {
        nthread = 0;
        for(int i = 0; i < NUM_THREAD; ++i) {
            argv[i].right = right;
            argv[i].rcid = rcid;
            argv[i].hash_table = &hash_table;
            *init_finish[i] = 0;
        }

        pthread_mutex_lock(&mutex);
        pthread_cond_broadcast(&cond);
        pthread_mutex_unlock(&mutex);

        while(true) {
            if(nthread == NUM_THREAD)
                break;
            pthread_yield();
        }

        for(int i = 0; i < NUM_THREAD; ++i) {
            for(int j = 0; j < argv[i].new_tuple->size(); ++j)
                ret_table.tuple.push_back(move((*argv[i].new_tuple)[j]));
            new_num_key += *init_finish[i];
        }

        ret_table.num_key = new_num_key;
        ret_table.is_valid = 1;
    }

    else {
        for(int i = 0; i < right->tuple.size(); ++i) {
            if(multi_left) {
                for(int j = 0; j < NUM_THREAD; ++j) {
                    auto its = hash_list[j].hash_table->equal_range(right->tuple[i][rcid]);
                    for(auto bucket_it = its.first; bucket_it != its.second; ++bucket_it) {
                        vector<int64_t> tmp = bucket_it->second + right->tuple[i];
                        ret_table.tuple.push_back(move(tmp));
                        new_num_key++;
                    }
                }
            }
            else {
                auto its = hash_table.equal_range(right->tuple[i][rcid]);
                for(auto bucket_it = its.first; bucket_it != its.second; ++bucket_it) {
                    vector<int64_t> tmp = bucket_it->second + right->tuple[i];
                    ret_table.tuple.push_back(move(tmp));
                    new_num_key++;
            }
            }
        }

        ret_table.num_key = new_num_key;
        ret_table.is_valid = 1;
    }

    //debug
    /*
    cout << "===========new_tuple===========" << endl;
    for(auto it = ret_table.tuple.begin(); it != ret_table.tuple.end(); ++it) {
        for(auto i = it->begin(); i != it->end(); ++i)
            cout << *i << " ";
        cout << endl;
    }
    */
    return ret_table;
}

int get_cid(const vector<int64_t>& v, int tid, int cid)
{
    int ret = 0;
    int i = 0;
    while(i < v.size()) {
        if(v[i] == tid) {
            ret += cid;
            break;
        }
        ret += (cat_pointer[v[i]]->num_column + 1);
        i += (cat_pointer[v[i]]->num_column + 1);
    }

    return ret;
}

int64_t get_result(const select_table* root)
{
    int64_t sum = 0;
    int i, j;
    for(i = 0; i < root->tuple.size(); ++i) {
        for(j = 0; j < root->tuple[i].size(); j += (cat_pointer[root->tuple[i][j]]->num_column + 1))
            sum += root->tuple[i][j + 1];
    }
    return sum;
}

int64_t join(const char* query)
{
    vector<join_table> ops = parse_query(query);
    if(ops.empty())
        return 0;

    vector<vector<join_table>::iterator> optimized_ops;
    optimized_ops = query_optimizer(ops);

    select_table root = (*optimized_ops.begin())->left_table;
    auto it = optimized_ops.begin();
    while(it != optimized_ops.end()) {
        if(m_table[(*it)->right_table.tid].tuple.empty())
            return 0;
        if(root.num_key < (*it)->right_table.num_key)
            root = operate_join(&root, &(*it)->right_table);
        else
            root = operate_join(&(*it)->right_table, &root);
        if(root.tuple.empty())
            return 0;
        if(it + 1 != optimized_ops.end()) {
            root.tid = (*(it + 1))->left_table.tid;
            root.cid = (*(it + 1))->left_table.cid;
        }
        it++;
    }

    return get_result(&root);
}

