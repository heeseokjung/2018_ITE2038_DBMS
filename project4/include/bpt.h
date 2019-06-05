#ifndef __BPT_H__
#define __BPT_H__

#include <stdlib.h>
#include <stdbool.h>
#include "disk_manager.h"
#include "buffer_manager.h"

// Every node has at most order - 1 keys
extern const int leaf_order;
extern const int internal_order;

// min_keys value for implementing delayed merging
extern int min_keys;

// FUNCTION PROTOTYPES.

// init / shutdown
int init_db (int num_buf);
int shutdown_db();
int open_table (char* pathname, int num_column);
int close_table(int table_id);

// Utillity and find functions
pagenum_t find_leaf(int tid, int64_t key);
int64_t* find(int table_id, int64_t key);
int cut( int length );

// Insertion.
record_t* make_record(int64_t key, int64_t* values, int tid);
index_t* make_index(int64_t key, pagenum_t pgnum);
page_t* make_internal();
page_t* make_leaf();
void insert_into_leaf(buf_frame* buf_leaf, int64_t key, record_t* record);
void insert_into_leaf_after_splitting(int tid, buf_frame* buf_leaf, int64_t key, record_t* record);
void insert_into_node(buf_frame* buf_parent, pagenum_t right_pgnum, int64_t key);
void insert_into_node_after_splitting(int tid, buf_frame* buf_parent, pagenum_t right_pgnum, int64_t key); 
void insert_into_parent(int tid, pagenum_t leftpg_num, pagenum_t rightpg_num, int64_t key);
void insert_into_new_root(int tid, buf_frame* buf_left, buf_frame* buf_right, int64_t key);
void start_new_tree(int tid, int64_t key, record_t* record);
int insert(int table_id, int64_t key, int64_t* values);

// Deletion.
int get_neighbor_index(page_t* parent, pagenum_t child_pgnum);
void adjust_root(int tid, buf_frame* buf_root);
void coalesce_nodes
(int tid, buf_frame* buf_node, buf_frame* buf_parent, buf_frame* buf_neighbor, int neighbor_index, int64_t k_prime_key);
void redistribute_nodes
(int tid, buf_frame* buf_node, buf_frame* buf_parent, buf_frame* neighbor, int neighbor_index, int k_prime_index);
void remove_entry_from_node(page_t* n, int64_t key);
void delete_entry(int tid, pagenum_t page_num, int64_t key) ;
int erase(int table_id, int64_t key);

// For debugging

typedef struct queue
{
    pgoffset_t* arr;
    int f;
    int r;
}queue;

void enqueue(pgoffset_t offset, queue* q);
pgoffset_t dequeue(queue* q);
void print_tree();


#endif /* __BPT_H__*/
