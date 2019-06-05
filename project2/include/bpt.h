#ifndef __BPT_H__
#define __BPT_H__

#include <stdlib.h>
#include <stdbool.h>
#include "disk_manager.h"


// Every node has at most order - 1 keys
extern int leaf_order;
extern int internal_order;

// min_keys value for implementing delayed merging
extern int min_keys;

// FUNCTION PROTOTYPES.

// Open disk file
int open_db(char* pathname);

// Utillity and find functions
pagenum_t find_leaf(int64_t key);
char* find(int64_t key);
int cut( int length );

// Insertion.
record_t * make_record(int64_t key, char* value);
index_t* make_index(int64_t key, pagenum_t pgnum);
page_t* make_internal();
page_t* make_leaf();
void insert_into_leaf(page_t* leaf, int64_t key, record_t* record);
void insert_into_leaf_after_splitting(page_t* leaf, int64_t key, record_t* record);
void insert_into_node(page_t* parent, pagenum_t right_pgnum, int64_t key);
void insert_into_node_after_splitting(page_t* parent, pagenum_t right_pgnum, int64_t key); 
void insert_into_parent(page_t* left, page_t* right, int64_t key);
void insert_into_new_root(page_t* left, page_t* right, int64_t key);
void start_new_tree(int64_t key, record_t* record);
int insert(int64_t key, char* value);

// Deletion.
int get_neighbor_index(page_t* parent, pagenum_t child_pgnum);
void adjust_root(page_t* root);
void coalesce_nodes
(page_t* n, page_t* parent, page_t* neighbor, int neighbor_index, int64_t k_prime_key);
void redistribute_nodes
(page_t* n, page_t* parent, page_t* neighbor, int neighbor_index, int k_prime_index);
void remove_entry_from_node(page_t* n, int64_t key);
void delete_entry(page_t* node, int64_t key);
int delete(int64_t key);

// For debugging
/*
typedef struct queue
{
    pgoffset_t* arr;
    int f;
    int r;
}queue;

void enqueue(pgoffset_t offset, queue* q);
pgoffset_t dequeue(queue* q);
void print_tree();
*/

#endif /* __BPT_H__*/
