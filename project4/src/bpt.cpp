// disk-based B+ tree

#include "sys_catalog.h"
#include "bpt.h"
#include "disk_manager.h"
#include "buffer_manager.h"
#include "join.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <vector>

// Every node has at most order - 1 keys
const int leaf_order = 32;
const int internal_order = 249;

// min_keys value for implementing delayed merging
int min_keys = 1;

// Return the offset of the node which has given key
pagenum_t find_leaf(int tid, int64_t key)
{
    header = buf_get_page(tid, 0);
    pagenum_t root = PGNUM(header->frame->rootpg_offset);
    if(root == 0)
    {
        buf_put_page(header, 0);
        return 0;
    }

    pagenum_t n = root;
    buf_frame* buf_node = buf_get_page(tid, n);
    page_t* node = buf_node->frame;
    while(!node->is_leaf)
    {
        int idx = -1;
        while(node->index[idx+1].key <= key && idx + 1 < node->num_keys)
            idx++;
        if(idx == -1)
            n = PGNUM(node->far_left);
        else
            n = PGNUM(node->index[idx].offset);
        buf_put_page(buf_node, 0);
        buf_node = buf_get_page(tid, n);
        node = buf_node->frame;
    }

    buf_put_page(header, 0);
    buf_put_page(buf_node, 0);

    return n;
}


// Finds and returns the record to which a key refers.
int64_t* find(int table_id, int64_t key)
{
    if(cat_pointer[table_id] == NULL)
        return NULL;
    pagenum_t c = find_leaf(table_id, key);
    if(c == 0)
        return NULL;
    buf_frame* buf_node =  buf_get_page(table_id, c);
    page_t* node = buf_node->frame;

    int i;
    for(i = 0; i < node->num_keys; ++i)
        if(node->record[i].key == key) break;
    if(i == node->num_keys)
    {
        buf_put_page(buf_node, 0);
        return NULL;
    }
    else
    {
        int64_t* ret = (int64_t*)malloc(sizeof(int64_t) * (cat_pointer[table_id]->num_column - 1));
        memcpy(ret, node->record[i].values, sizeof(int64_t) * (cat_pointer[table_id]->num_column - 1));
        buf_put_page(buf_node, 0);
        return ret;
    }
}

// Finds the appropriate place to split a node
int cut( int length ) {
    if (length % 2 == 0)
        return length/2;
    else
        return length/2 + 1;
}


// INSERTION

// Creates a new record
record_t* make_record(int64_t key, int64_t* values, int tid) 
{
    record_t* new_record;
    new_record = (record_t*)malloc(sizeof(record_t));
    if(new_record == NULL)
    {
        perror("creating record failed.\n");
        exit(EXIT_FAILURE);
    }
    else
    {
        new_record->key = key;
        memcpy(new_record->values, values, sizeof(int64_t) * (cat_pointer[tid]->num_column - 1));
    }
    return new_record;
}

// Creates a new entry in the internal node
index_t* make_index(int64_t key, pagenum_t pgnum)
{
    index_t* new_index;
    new_index = (index_t*)malloc(sizeof(index_t));
    if(new_index == NULL)
    {
        perror("creating index failed.\n");
        exit(EXIT_FAILURE);
    }
    else
    {
        new_index->key = key;
        new_index->offset = PGOFFSET(pgnum);
    }
    return new_index;
}

// Ceates a new leaf node
page_t* make_leaf() 
{
    page_t* new_leaf;
    new_leaf = (page_t*)malloc(sizeof(page_t));
    if(new_leaf == NULL)
    {
        perror("Making new leaf node failed.\n");
        exit(EXIT_FAILURE);
    }
    new_leaf->parent = 0;
    new_leaf->is_leaf = 1;
    new_leaf->num_keys = 0;
    new_leaf->sibling = 0;
    return new_leaf;
}

// Creates new internal node
page_t* make_internal()
{
    page_t* new_internal;
    new_internal = (page_t*)malloc(sizeof(page_t));
    if(new_internal == NULL)
    {
        perror("Making new internal node failed.\n");
        exit(EXIT_FAILURE);
    }
    new_internal->parent = 0;
    new_internal->is_leaf = 0;
    new_internal->num_keys = 0;
    new_internal->far_left = 0;
    return new_internal;
}

/* Inserts a new pointer to a record and its corresponding
 * key into a leaf.
 */
void insert_into_leaf(buf_frame* buf_leaf, int64_t key, record_t* record)
{
    page_t* leaf = buf_leaf->frame;
    int i, ip = 0;
    while(ip < leaf->num_keys && leaf->record[ip].key < key)
        ip++;
    for(i = leaf->num_keys; i > ip; --i)
        memcpy(&leaf->record[i], &leaf->record[i-1], sizeof(record_t));
    memcpy(&leaf->record[ip], record, sizeof(record_t));
    leaf->num_keys++;

    buf_put_page(buf_leaf, 1);
}


/* Inserts a new key and pointer
 * to a new record into a leaf so as to exceed
 * the tree's order, causing the leaf to be split
 * in half.
 */
void insert_into_leaf_after_splitting(int tid, buf_frame* buf_leaf, int64_t key, record_t* record) 
{
    page_t* leaf = buf_leaf->frame;
    page_t* new_leaf = make_leaf();
    buf_frame* buf_new_leaf = buf_alloc_page(tid, new_leaf);
    record_t* tmp_record;
    tmp_record = (record_t*)malloc(sizeof(record_t) * leaf_order);
    if(tmp_record == NULL)
    {
        perror("Making temporary array failed.\n");
        exit(EXIT_FAILURE);
    }

    int ip = 0;
    while(ip < leaf_order - 1 && leaf->record[ip].key < key)
        ip++;

    for(int i = 0, j = 0; i < leaf->num_keys; ++i, ++j)
    {
        if(j == ip) j++;
        memcpy(&tmp_record[j], &leaf->record[i], sizeof(record_t));
    }
    memcpy(&tmp_record[ip], record, sizeof(record_t));

    leaf->num_keys = 0;
    int split = cut(leaf_order - 1);
    for(int i = 0; i < split; ++i)
    {
        memcpy(&leaf->record[i], &tmp_record[i], sizeof(record_t));
        leaf->num_keys++;
    }
    for(int i = split, j = 0; i < leaf_order; ++i, ++j)
    {
        memcpy(&new_leaf->record[j], &tmp_record[i], sizeof(record_t));
        new_leaf->num_keys++;
    }
    free(tmp_record);

    new_leaf->my_offset = PGOFFSET(buf_new_leaf->page_num);
    new_leaf->sibling = leaf->sibling;
    leaf->sibling = PGOFFSET(buf_new_leaf->page_num);
    new_leaf->parent = leaf->parent;
    int64_t copy_key = new_leaf->record[0].key;
    
    pagenum_t leafpg_num = PGNUM(leaf->my_offset);
    pagenum_t new_leafpg_num = PGNUM(new_leaf->my_offset);
    buf_put_page(buf_leaf, 1);
    buf_put_page(buf_new_leaf, 1);

    insert_into_parent(tid, leafpg_num, new_leafpg_num, copy_key);
}


/* Inserts a new key and pointer to a node
 * into a node into which these can fit
 * without violating the B+ tree properties.
 */
void insert_into_node(buf_frame* buf_parent, pagenum_t right_pgnum, int64_t key) 
{
    page_t* parent = buf_parent->frame;
    int ip = 0;
    while(ip < parent->num_keys && parent->index[ip].key < key)
        ip++;
    for(int i = parent->num_keys; i > ip; --i)
        memcpy(&parent->index[i], &parent->index[i-1], sizeof(index_t));
    index_t* new_index;
    new_index = make_index(key, right_pgnum);
    memcpy(&parent->index[ip], new_index, sizeof(index_t));
    parent->num_keys++;

    buf_put_page(buf_parent, 1);
    free(new_index);
}


/* Inserts a new key and pointer to a node
 * into a node, causing the node's size to exceed
 * the order, and causing the node to split into two.
 */
void insert_into_node_after_splitting(int tid, buf_frame* buf_parent, pagenum_t right_pgnum, int64_t key) 
{
    page_t* parent = buf_parent->frame;
    index_t* tmp_index = (index_t*)malloc(sizeof(index_t) * internal_order);
    index_t* new_index = make_index(key, right_pgnum);

    int ip = 0;
    while(ip < internal_order - 1 && parent->index[ip].key < key)
        ip++;
    for(int i = 0, j = 0; i < internal_order - 1; ++i, ++j)
    {
        if(j == ip) j++;
        memcpy(&tmp_index[j], &parent->index[i], sizeof(index_t));
    }
    memcpy(&tmp_index[ip], new_index, sizeof(index_t));

    page_t* new_internal = make_internal();
    buf_frame* buf_new_internal = buf_alloc_page(tid, new_internal);
    parent->num_keys = 0;
    int split = cut(internal_order);
    for(int i = 0; i < split - 1; ++i)
    {
        memcpy(&parent->index[i], &tmp_index[i], sizeof(index_t));
        parent->num_keys++;
    }
    int push_up_key = tmp_index[split-1].key;
    for(int i = split, j = 0; i < internal_order; ++i, ++j)
    {
        memcpy(&new_internal->index[j], &tmp_index[i], sizeof(index_t));
        new_internal->num_keys++;
    }
    new_internal->far_left = tmp_index[split-1].offset;
    new_internal->parent = parent->parent;
    new_internal->my_offset = PGOFFSET(buf_new_internal->page_num);

    pagenum_t left = buf_parent->page_num;
    pagenum_t right = buf_new_internal->page_num;
    buf_put_page(buf_parent, 1);
    // Modify child's offset to parent
    buf_frame* buf_child = buf_get_page(tid, PGNUM(new_internal->far_left));
    buf_child->frame->parent = PGOFFSET(buf_new_internal->page_num);
    buf_put_page(buf_child, 1);
    for(int i = 0; i < new_internal->num_keys; ++i)
    {
        buf_child = buf_get_page(tid, PGNUM(new_internal->index[i].offset));
        buf_child->frame->parent = PGOFFSET(buf_new_internal->page_num);
        buf_put_page(buf_child, 1);
    }
    buf_put_page(buf_new_internal, 1);

    free(tmp_index);
    free(new_index);

    insert_into_parent(tid, left, right, push_up_key);
}

// Propagate to parent
void insert_into_parent(int tid, pagenum_t leftpg_num, pagenum_t rightpg_num, int64_t key) 
{
    buf_frame* buf_left = buf_get_page(tid, leftpg_num);
    buf_frame* buf_right = buf_get_page(tid, rightpg_num);
    page_t* left = buf_left->frame;
    page_t* right = buf_right->frame;

    if (left->parent == 0) 
        return insert_into_new_root(tid, buf_left, buf_right, key);

    buf_frame* buf_parent = buf_get_page(tid, PGNUM(left->parent));

    // Simple case: the new key fits into the node. 
    if (buf_parent->frame->num_keys < internal_order - 1)
    {
        buf_put_page(buf_left, 1);
        buf_put_page(buf_right, 1);
        return insert_into_node(buf_parent, rightpg_num, key);
    }

    /* Harder case:  split a node in order 
     * to preserve the B+ tree properties.
     */
    buf_put_page(buf_left, 1);
    buf_put_page(buf_right, 1);
    insert_into_node_after_splitting(tid, buf_parent, rightpg_num, key);
}


/* Creates a new root for two subtrees
 * and inserts the appropriate key into
 * the new root.
 */
void insert_into_new_root(int tid, buf_frame* buf_left, buf_frame* buf_right, int64_t key)
{
    page_t* left = buf_left->frame;
    page_t* right = buf_right->frame;
    page_t* root = make_internal();
    buf_frame* buf_root = buf_alloc_page(tid, root);
    root->index[0].key = key;
    root->index[0].offset = PGOFFSET(buf_right->page_num);
    root->far_left = PGOFFSET(buf_left->page_num);
    root->num_keys++;
    left->parent = PGOFFSET(buf_root->page_num);
    right->parent = PGOFFSET(buf_root->page_num);
    root->my_offset = PGOFFSET(buf_root->page_num);
    header = buf_get_page(tid, 0);
    header->frame->rootpg_offset = PGOFFSET(buf_root->page_num);
    
    buf_put_page(header, 1);
    buf_put_page(buf_left, 1);
    buf_put_page(buf_right, 1);
    buf_put_page(buf_root, 1);
}

// Start a new tree
void start_new_tree(int tid, int64_t key, record_t* record)
{
    page_t* root = make_leaf();
    buf_frame* buf_root = buf_alloc_page(tid, root);
    memcpy(&root->record[0], record, sizeof(record_t));
    root->num_keys++;
    root->my_offset = PGOFFSET(buf_root->page_num);
    header = buf_get_page(tid, 0);
    header->frame->rootpg_offset = PGOFFSET(buf_root->page_num);
    buf_put_page(buf_root, 1);
    buf_put_page(header, 1);
    free(record);
}

// Master insertion function
int insert(int table_id, int64_t key, int64_t* values) 
{
    if(cat_pointer[table_id] == NULL)
        return -1;
    record_t* new_record;
    buf_frame* buf_leaf;
    pagenum_t leafpg_num;
    int64_t* ret;

    // Ignores duplicate
    if((ret = find(table_id, key)) != NULL)
    {
        free(ret);
        return -1;
    }

    /* Create a new record for the
     * value.
     */
    new_record = make_record(key, values, table_id);

    /* Case: the tree does not exist yet.
     * Start a new tree.
     */
    header = buf_get_page(table_id, 0);
    pagenum_t rootpg_num = PGNUM(header->frame->rootpg_offset);
    buf_put_page(header, 0);
    if (rootpg_num == 0)
    { 
        start_new_tree(table_id, key, new_record);
        free(ret);
        return 0;
    }

    /* Case: the tree already exists.
     * (Rest of function body.)
     */
    leafpg_num = find_leaf(table_id, key);
    buf_leaf = buf_get_page(table_id, leafpg_num);

    /* Case: leaf has room for key and pointer.
     */
    if (buf_leaf->frame->num_keys < leaf_order - 1) 
        insert_into_leaf(buf_leaf, key, new_record);

    /* Case:  leaf must be split.
     */
    else
        insert_into_leaf_after_splitting(table_id, buf_leaf, key, new_record);
    
    free(new_record);
    free(ret);

    return 0;
}

// DELETION.

/* Utility function for deletion.  Retrieves
 * the index of a node's nearest neighbor (sibling)
 * to the left if one exists.  If not (the node
 * is the leftmost child), returns -2 and when 
 * the node is child of first entry of parent 
 * to signify this special case.
 */
int get_neighbor_index(page_t* parent, pagenum_t child_pgnum)
{
    int i;
    if(parent->far_left == PGOFFSET(child_pgnum))
        return -2;
    for(i = 0; i < parent->num_keys; ++i)
        if(parent->index[i].offset == PGOFFSET(child_pgnum))
            return i - 1;

    // Error state.
    printf("Search for nonexistent pointer to node in parent.\n");
    printf("Node:  %ld\n", child_pgnum);
    exit(EXIT_FAILURE);
}


void remove_entry_from_node(page_t* n, int64_t key) 
{   
    int i = 0;
    if(n->is_leaf)
    {
        while(n->record[i].key != key)
            i++;
        for(++i; i < n->num_keys; ++i)
            memcpy(&n->record[i-1], &n->record[i], sizeof(record_t));
        n->num_keys--;
    }
    else
    {
        while(n->index[i].key != key)
            i++;
        for(++i; i < n->num_keys; ++i)
            memcpy(&n->index[i-1], &n->index[i], sizeof(index_t));
        n->num_keys--;
    }
}


void adjust_root(int tid, buf_frame* buf_root)
{
    page_t* root = buf_root->frame;

    // Case: nonempty root
    if(root->num_keys > 0)
        return buf_put_page(buf_root, 1);

    // Case: empty root & it has child
    header = buf_get_page(tid, 0);
    if(!root->is_leaf)
    {
        header->frame->rootpg_offset = root->far_left;
        buf_frame* buf_new_root = buf_get_page(tid, PGNUM(header->frame->rootpg_offset));
        buf_new_root->frame->parent = 0;
        buf_put_page(buf_new_root, 1);
        buf_put_page(header, 1);
        buf_free_page(buf_root);
    }
    
    // Case: empty root & no child
    else
    {
        header->frame->rootpg_offset = 0;
        buf_put_page(header, 1);
        buf_free_page(buf_root);
    }
}


// Merging node when negihbor has enough space to merge
void coalesce_nodes
(int tid, buf_frame* buf_node, buf_frame* buf_parent, buf_frame* buf_neighbor, int neighbor_index, int64_t k_prime_key)
{
    page_t* n = buf_node->frame;
    page_t* parent = buf_parent->frame;
    page_t* neighbor = buf_neighbor->frame;
    int i, j, changed = 0;
    index_t* k_prime_index;
    // Swap neighbor with node if node is on the far left
    if(neighbor_index == -2)
    {
        page_t* tmp;
        tmp = n;
        n = neighbor;
        neighbor = tmp;
        changed = 1;
    }

    // Case: leaf node
    if(n->is_leaf)
    {
        for(i = 0, j = neighbor->num_keys; i < n->num_keys; ++i, ++j)
        {
            memcpy(&neighbor->record[j], &n->record[i], sizeof(record_t));
            neighbor->num_keys++;
        }
        neighbor->sibling = n->sibling;
    }

    // Case: internal node
    else
    {
        k_prime_index = make_index(k_prime_key, PGNUM(n->far_left));
        memcpy(&neighbor->index[neighbor->num_keys], k_prime_index, sizeof(index_t));
        neighbor->num_keys++;
        for(i = 0, j = neighbor->num_keys; i < n->num_keys; ++i, ++j)
        {
            memcpy(&neighbor->index[j], &n->index[i], sizeof(index_t));
            neighbor->num_keys++;
        }
        // Modify child's offset to parent
        buf_frame* buf_child;
        for(i = 0; i < neighbor->num_keys; ++i)
        {
            buf_child = buf_get_page(tid, PGNUM(neighbor->index[i].offset));
            buf_child->frame->parent = neighbor->my_offset;
            buf_put_page(buf_child, 1);
        }
        free(k_prime_index);
    }

    pagenum_t parent_pgnum = buf_parent->page_num;
    if(changed) {
        buf_put_page(buf_node, 1);
        buf_free_page(buf_neighbor);
    }
    else {
        buf_put_page(buf_neighbor, 1);
        buf_free_page(buf_node);
    }
    buf_put_page(buf_parent, 1);
    
    delete_entry(tid, parent_pgnum, k_prime_key);
}

// Redistribute when neighbor doesn't have enough space to merge
void redistribute_nodes
(int tid, buf_frame* buf_node, buf_frame* buf_parent, buf_frame* buf_neighbor, int neighbor_index, int k_prime_index)
{
    page_t* n = buf_node->frame;
    page_t* parent = buf_parent->frame;
    page_t* neighbor = buf_neighbor->frame;
    pagenum_t k_prime_pgnum;
    index_t* k_prime;

    // Case: n is left-most child
    if(neighbor_index == -2)
    {
        k_prime_pgnum = PGNUM(neighbor->far_left);
        k_prime = make_index(parent->index[k_prime_index].key, k_prime_pgnum);
        // Move left-end key of the neighbor to parent
        memcpy(&parent->index[k_prime_index], &neighbor->index[0], sizeof(index_t));
        parent->index[k_prime_index].offset = neighbor->my_offset;
        // Make copy of the k_prime to left-end of the n
        memcpy(&n->index[0], k_prime, sizeof(index_t));
        n->num_keys++;
        free(k_prime);
        // Link neighbor's far_left->parent to n
        buf_frame* buf_far_left = buf_get_page(tid, k_prime_pgnum);
        buf_far_left->frame->parent = n->my_offset;
        buf_put_page(buf_far_left, 1);
        // Pull all the keys left in neighbor
        neighbor->far_left = neighbor->index[0].offset;
        for(int i = 0; i < neighbor->num_keys - 1; ++i)
            memcpy(&neighbor->index[i], &neighbor->index[i+1], sizeof(index_t));
        neighbor->num_keys--;
    }

    // Case: general(neighbor is left of n)
    else
    {
        k_prime_pgnum = PGNUM(n->far_left);
        k_prime = make_index(parent->index[k_prime_index].key, k_prime_pgnum);
        // Move right-end key of the neighbor to parent
        memcpy(&parent->index[k_prime_index], &neighbor->index[neighbor->num_keys-1], sizeof(index_t));
        parent->index[k_prime_index].offset = n->my_offset;
        pagenum_t far_right = PGNUM(neighbor->index[neighbor->num_keys-1].offset);
        neighbor->num_keys--;
        // Move copy of the k_prime to the left-end of the n
        memcpy(&n->index[0], k_prime, sizeof(index_t));
        n->num_keys++;
        free(k_prime);
        // Connecting offset
        n->far_left = PGOFFSET(far_right);
        // Link neighbor's far_right->parent to n
        buf_frame* buf_far_right = buf_get_page(tid, far_right);
        buf_far_right->frame->parent = n->my_offset;
        buf_put_page(buf_far_right, 1);
    }

    buf_put_page(buf_node, 1);
    buf_put_page(buf_parent, 1);
    buf_put_page(buf_neighbor, 1);
}

// Delete entry of the node
void delete_entry(int tid, pagenum_t page_num, int64_t key) 
{
    buf_frame* buf_node = buf_get_page(tid, page_num);
    page_t* node = buf_node->frame;

    // First remove entry form node
    remove_entry_from_node(node, key);

    // Case: deletion frmo the root
    header = buf_get_page(tid, 0);
    pagenum_t rootpg_num = PGNUM(header->frame->rootpg_offset);
    buf_put_page(header, 0);
    if(rootpg_num == page_num)
        return adjust_root(tid, buf_node);

    /* Case: deletion from a node below root */
    
    // Case: node stays at or above minimum
    if(node->num_keys >= min_keys)
        return buf_put_page(buf_node, 1);

    /* Case: node falls below minimum */
    buf_frame* buf_parent = buf_get_page(tid, PGNUM(node->parent));
    page_t* parent = buf_parent->frame;

    int neighbor_index = get_neighbor_index(parent, page_num);
    int k_prime_index;
    if(neighbor_index == -2 || neighbor_index == -1)
        k_prime_index = 0;
    else
        k_prime_index = neighbor_index + 1;
    int64_t k_prime = parent->index[k_prime_index].key;
    pagenum_t neighbor_pgnum;
    switch(neighbor_index)
    {
        case -2:
            neighbor_pgnum = PGNUM(parent->index[0].offset);
            break;
        case -1:
            neighbor_pgnum = PGNUM(parent->far_left);
            break;
        default:
            neighbor_pgnum = PGNUM(parent->index[neighbor_index].offset);
            break;
    }

    buf_frame* buf_neighbor = buf_get_page(tid, neighbor_pgnum);
    page_t* neighbor = buf_neighbor->frame;

    int capacity = node->is_leaf ? leaf_order : internal_order - 1;

    // Case: merging
    if(neighbor->num_keys + node->num_keys < capacity)
        return coalesce_nodes(tid, buf_node, buf_parent, buf_neighbor, neighbor_index, k_prime);
    
    // Case: redistribution
    return redistribute_nodes(tid, buf_node, buf_parent, buf_neighbor, neighbor_index, k_prime_index); 
}



// Master deletion function
int erase(int table_id, int64_t key)
{
    if(cat_pointer[table_id] == NULL)
        return -1;
    int64_t* value;
    value = find(table_id, key);

    if(value == NULL)
    {
        printf("The value to key %ld doesn't exist.\n", key);
        return -1;
    }
 
    pagenum_t leafpg_num = find_leaf(table_id, key);

    delete_entry(table_id, leafpg_num, key);
    
    free(value);
    return 0;
}

int init_db(int num_buf)
{
    // Initializing system catalog
    init_cat();

    // Initializing in-memory tables for Join
    for(int i = 0; i <= NUM_TABLES; ++i)
        m_table[i].is_valid = 0;

    // Initializing buffer pool
    buf_header = (buf_frame*)malloc(sizeof(buf_frame));
    if(buf_header == NULL)
    {
        printf("Allocating buffer pool failed.\n");
        return -1;
    }
    buf_header->next = NULL;
    buf_frame* p = buf_header;
    for(int i = 1; i <= num_buf; ++i)
    {
        buf_frame* buf = (buf_frame*)malloc(sizeof(buf_frame));
        buf->frame = NULL;
        buf->table_id = -1;
        buf->page_num = -1;
        buf->is_dirty = 0;
        buf->pin_count = 0;
        buf->next = p->next;
        p->next = buf;
        buf->prev = p;
        p = buf;
        if(i == num_buf)
            buf_header->last = buf;
    }

    // Creating thread pool
    // For Hashing left
    for(int i = 0; i < NUM_THREAD; ++i)
        multi_hash_init[i] = false;
    for(int i = 0; i < NUM_THREAD; ++i) {
        hash_list[i].thread_id = i;
        if(pthread_create(&left_threads[i], 0, multithread_hash_left_table, (void*)&hash_list[i]) < 0) {
            printf("Creating thread pool failed.\n");
            exit(EXIT_FAILURE);
        }
        while(multi_hash_init[i] == false)
            pthread_yield();
    }

    // For Join right
    for(int i = 0; i < NUM_THREAD; ++i)
        init_finish[i] = NULL;
    for(int i = 0; i < NUM_THREAD; ++i) {
        argv[i].thread_id = i;
        if(pthread_create(&threads[i], 0, multithread_join_right_table, (void*)&argv[i]) < 0) {
            printf("Creating thread pool failed.\n");
            exit(EXIT_FAILURE);
        }
        while(init_finish[i] == NULL)
            pthread_yield();
    }

    return 0;
}

int shutdown_db()
{
    if(buf_header == NULL)
        return -1;
    // Shutdown buffer pool
    buf_frame* p = buf_header->next;
    while(p != NULL)
    {
        if(p->is_dirty)
            file_write_page(p->table_id, p->page_num, p->frame);
        buf_header->next = p->next;
        free(p);
        p = buf_header->next;
    }
    free(buf_header);

    // Shutdown system catalog
    shutdown_cat();

    // Shutdown thread pool
    shutdown_all_thread = true;
    pthread_cond_broadcast(&cond);
    for(int i = 0; i < NUM_THREAD; ++i) {
        pthread_join(threads[i], NULL);
        pthread_join(left_threads[i], NULL);
    }

    return 0;
}

// first page -> header?
int open_table(char* pathname, int num_column)
{
    if(buf_header == NULL) {
        printf("init_db not has been called.\n");
        return -1;
    }
    if(num_column < 2 || num_column > 16) {
        printf("Invalid column number.\n");
        return -1;
    }

    int tid, new_file = 0;
    fp = fopen(pathname, "r+b");
    if(fp == NULL) {
        fp = fopen(pathname, "w+b");
        new_file = 1;
    }
    assert(fp != NULL);
    tid = mapping_cat(pathname, num_column);
    cat_pointer[tid]->fp = fp;
    if(new_file) {
        page_t* tmp_header = (page_t*)malloc(sizeof(page_t));
        tmp_header->freepg_offset = 0;
        tmp_header->rootpg_offset = 0;
        tmp_header->num_pages = 1;
        tmp_header->num_columns = num_column;
        file_write_page(tid, 0, tmp_header);
        free(tmp_header);
    }
    else {
        header = buf_get_page(tid, 0);
        cat_pointer[tid]->num_column = header->frame->num_columns;
        buf_put_page(header, 0);
    }

    /* preprocessing */
    pre_scan_table(tid);

    return tid;
}

// Unlink 
int close_table(int table_id)
{
    if(table_id < 1 || table_id > NUM_TABLES) {
        printf("Invalid table id.\n");
        return -1;
    }
    if(cat_pointer[table_id] == NULL) {
        printf("Not opened file.\n");
        return -1;
    }

    // Close buffer entry
    buf_frame* buf = buf_header->next;
    while(buf != NULL)
    {
        if(buf->table_id == table_id)
        {
            if(buf->is_dirty)
                file_write_page(buf->table_id, buf->page_num, buf->frame);
            if(buf->frame != NULL)
                free(buf->frame);
            buf->frame = NULL;
            buf->table_id = -1;
            buf->page_num = -1;
            buf->is_dirty = 0;
            buf->pin_count = 0;
        }
        buf = buf->next;
    }
    
    // Close sys_catalog entry
    reset_cat(table_id);
    cat_pointer[table_id] = NULL;
    return 0;
}

// for debugging
/*
int get_rank(pgoffset_t offset)
{
    page_t* pg = (page_t*)malloc(sizeof(page_t));
    file_read_header();
    pgoffset_t next_offset = offset;
    int rank = 0;
    while(next_offset != header.rootpg_offset)
    {
        file_read_page(next_offset, pg);
        next_offset = pg->parent;
        rank++;
    }
    free(pg);
    return rank;
}
void enqueue(pgoffset_t offset, queue* q) 
{ 
    q->arr[++q->r] = offset;
}
pgoffset_t dequeue(queue* q)
{
    return q->arr[q->f++];
}


void print_tree()
{
    file_read_header();
    if(header.rootpg_offset == 0)
    {
        printf("Empty tree!\n");
        return;
    }
    queue* q = (queue*)malloc(sizeof(queue));
    q->arr = (pgoffset_t*)malloc(sizeof(pgoffset_t) * 100);
    q->f = 0;
    q->r = -1;
    
    enqueue(header.rootpg_offset, q);
    page_t page, parent_node;
    page_t* node = &page;
    page_t* parent = &parent_node;
    int new_rank = 0;
    while(q->f <= q->r)
    {
        pgoffset_t offset = dequeue(q);
        file_read_page(offset, node);
        if(node->parent != 0)
        {
            file_read_page(node->parent, parent);
            internal_t* internal;
            internal = (internal_t*)parent;
            if(get_rank(offset) != new_rank)
            {
                new_rank = get_rank(offset);
                printf("\n");
            }
        }
        if(!node->is_leaf)
        {
            internal_t* internal;
            internal = (internal_t*)node;
            for(int i = 0; i < internal->num_keys; ++i)
                printf("%ld  ", internal->entries[i].key);
            enqueue(internal->far_left, q);
            for(int i = 0; i < internal->num_keys; ++i)
                enqueue(internal->entries[i].offset, q);
        }
        else
        {
            leaf_t* leaf;
            leaf = (leaf_t*)node;
            for(int i = 0; i < leaf->num_keys; ++i)
                printf("%ld ", leaf->records[i].key,);
        }
        printf("| ");
    }
    printf("\n");
    free(q->arr);
    free(q);;
}
*/