// disk-based B+ tree

#include "bpt.h"
#include <string.h>

// Every node has at most order - 1 keys
int leaf_order = 32;
int internal_order = 249;

// min_keys value for implementing delayed merging
int min_keys = 1;

// Return the offset of the node which has given key
pagenum_t find_leaf(int64_t key)
{
    pagenum_t root = PGNUM(header->rootpg_offset);
    if(root == 0)
        return 0;

    pagenum_t n = root;
    page_t node;
    file_read_page(n, &node);
    while(!node.is_leaf)
    {
        int idx = -1;
        while(node.index[idx+1].key <= key && idx + 1 < node.num_keys)
            idx++;
        if(idx == -1)
            n = PGNUM(node.far_left);
        else
            n = PGNUM(node.index[idx].offset);
        file_read_page(n, &node);
    }
    return n;
}


// Finds and returns the record to which a key refers.
char* find(int64_t key)
{
    pagenum_t c = find_leaf(key);
    if(c == 0)
        return NULL;
    page_t node;
    file_read_page(c, &node);

    int i;
    for(i = 0; i < node.num_keys; ++i)
        if(node.record[i].key == key) break;
    if(i == node.num_keys)
        return NULL;
    else
    {
        char* tmp = (char*)malloc(sizeof(char*) * 120);
        memcpy(tmp, node.record[i].value, sizeof(char) * 120);
        return tmp;
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
record_t* make_record(int64_t key, char* value) 
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
        memcpy(new_record->value, value, sizeof(char) * 120);
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
void insert_into_leaf(page_t* leaf, int64_t key, record_t* record)
{
    pagenum_t leaf_pgnum = PGNUM(leaf->my_offset);
    int i, ip = 0;
    while(ip < leaf->num_keys && leaf->record[ip].key < key)
        ip++;
    for(i = leaf->num_keys; i > ip; --i)
        memcpy(&leaf->record[i], &leaf->record[i-1], sizeof(record_t));
    memcpy(&leaf->record[ip], record, sizeof(record_t));
    leaf->num_keys++;
    file_write_page(leaf_pgnum, leaf);
}


/* Inserts a new key and pointer
 * to a new record into a leaf so as to exceed
 * the tree's order, causing the leaf to be split
 * in half.
 */
void insert_into_leaf_after_splitting(page_t* leaf, int64_t key, record_t* record) 
{
    pagenum_t leaf_pgnum = PGNUM(leaf->my_offset);
    page_t* new_leaf;
    new_leaf = make_leaf();
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

    pagenum_t freepg_num = file_alloc_page();
    new_leaf->my_offset = PGOFFSET(freepg_num);
    new_leaf->sibling = leaf->sibling;
    leaf->sibling = PGOFFSET(freepg_num);
    new_leaf->parent = leaf->parent;
    int64_t copy_key = new_leaf->record[0].key;
    file_write_page(leaf_pgnum, leaf);
    file_write_page(freepg_num, new_leaf);

    insert_into_parent(leaf, new_leaf, copy_key);
    free(new_leaf);
}


/* Inserts a new key and pointer to a node
 * into a node into which these can fit
 * without violating the B+ tree properties.
 */
void insert_into_node(page_t* parent, pagenum_t right_pgnum, int64_t key) 
{
    pagenum_t parent_pgnum = PGNUM(parent->my_offset);
    int ip = 0;
    while(ip < parent->num_keys && parent->index[ip].key < key)
        ip++;
    for(int i = parent->num_keys; i > ip; --i)
        memcpy(&parent->index[i], &parent->index[i-1], sizeof(index_t));
    index_t* new_index;
    new_index = make_index(key, right_pgnum);
    memcpy(&parent->index[ip], new_index, sizeof(index_t));
    parent->num_keys++;

    file_write_page(parent_pgnum, parent);
    free(new_index);
}


/* Inserts a new key and pointer to a node
 * into a node, causing the node's size to exceed
 * the order, and causing the node to split into two.
 */
void insert_into_node_after_splitting(page_t* parent, pagenum_t right_pgnum, int64_t key) 
{
    index_t* tmp_index;
    index_t* new_index;
    tmp_index = (index_t*)malloc(sizeof(index_t) * internal_order);
    new_index = make_index(key, right_pgnum);

    int ip = 0;
    while(ip < internal_order - 1 && parent->index[ip].key < key)
        ip++;
    for(int i = 0, j = 0; i < internal_order - 1; ++i, ++j)
    {
        if(j == ip) j++;
        memcpy(&tmp_index[j], &parent->index[i], sizeof(index_t));
    }
    memcpy(&tmp_index[ip], new_index, sizeof(index_t));

    page_t* new_internal;
    new_internal = make_internal();
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

    pagenum_t freepg_num = file_alloc_page();
    pagenum_t parent_pgnum = PGNUM(parent->my_offset);
    new_internal->my_offset = PGOFFSET(freepg_num);
    file_write_page(parent_pgnum, parent);
    file_write_page(freepg_num, new_internal);
    // Modify child's offset to parent
    page_t child;
    file_read_page(PGNUM(new_internal->far_left), &child);
    child.parent = PGOFFSET(freepg_num);
    file_write_page(PGNUM(new_internal->far_left), &child);
    for(int i = 0; i < new_internal->num_keys; ++i)
    {
        file_read_page(PGNUM(new_internal->index[i].offset), &child);
        child.parent = PGOFFSET(freepg_num);
        file_write_page(PGNUM(new_internal->index[i].offset), &child);
    }

    free(tmp_index);
    free(new_index);
    insert_into_parent(parent, new_internal, push_up_key);
    free(new_internal);
}

// Propagate to parent
void insert_into_parent(page_t* left, page_t* right, int64_t key) 
{
    pagenum_t left_pgnum = PGNUM(left->my_offset);
    pagenum_t right_pgnum = PGNUM(right->my_offset);

    if (left->parent == 0) 
        return insert_into_new_root(left, right, key);

    page_t parent;
    file_read_page(PGNUM(left->parent), &parent);

    // Simple case: the new key fits into the node. 
    if (parent.num_keys < internal_order - 1)
        return insert_into_node(&parent, right_pgnum, key);


    /* Harder case:  split a node in order 
     * to preserve the B+ tree properties.
     */
    insert_into_node_after_splitting(&parent, right_pgnum, key);
}


/* Creates a new root for two subtrees
 * and inserts the appropriate key into
 * the new root.
 */
void insert_into_new_root(page_t* left, page_t* right, int64_t key)
{
    pagenum_t left_pgnum = PGNUM(left->my_offset);
    pagenum_t right_pgnum = PGNUM(right->my_offset);
    
    page_t* root;
    root = make_internal();
    root->index[0].key = key;
    root->index[0].offset = PGOFFSET(right_pgnum);
    root->far_left = PGOFFSET(left_pgnum);
    root->num_keys++;

    pagenum_t root_pgnum = file_alloc_page();
    left->parent = PGOFFSET(root_pgnum);
    right->parent = PGOFFSET(root_pgnum);
    root->my_offset = PGOFFSET(root_pgnum);
    header->rootpg_offset = PGOFFSET(root_pgnum);
    file_write_page(root_pgnum, root);
    file_write_page(left_pgnum, left);
    file_write_page(right_pgnum, right);
    file_write_page(0, header);
    free(root);
}



// Start a new tree
void start_new_tree(int64_t key, record_t* record)
{
    page_t* root = make_leaf();
    memcpy(&root->record[0], record, sizeof(record_t));
    root->num_keys++;

    pagenum_t root_pgnum = file_alloc_page();
    root->my_offset = PGOFFSET(root_pgnum);
    header->rootpg_offset = PGOFFSET(root_pgnum);
    file_write_page(root_pgnum, root);
    file_write_page(0, header);
    free(root);
    free(record);
}


// Master insertion function
int insert(int64_t key, char* value) 
{
    record_t* new_record;
    pagenum_t leaf_pgnum;
    page_t leaf;
    char* ret;

    // Ignores duplicate
    if((ret = find(key)) != NULL)
    {
        free(ret);
        return -1;
    }

    /* Create a new record for the
     * value.
     */
    new_record = make_record(key, value);


    /* Case: the tree does not exist yet.
     * Start a new tree.
     */
    if (header->rootpg_offset == 0)
    { 
        start_new_tree(key, new_record);
        free(ret);
        return 0;
    }

    /* Case: the tree already exists.
     * (Rest of function body.)
     */

    leaf_pgnum = find_leaf(key);
    file_read_page(leaf_pgnum, &leaf);

    /* Case: leaf has room for key and pointer.
     */
    if (leaf.num_keys < leaf_order - 1) 
    {
        insert_into_leaf(&leaf, key, new_record);
        free(new_record);
        free(ret);
        return 0;
    }

    /* Case:  leaf must be split.
     */
    insert_into_leaf_after_splitting(&leaf, key, new_record);
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
    if(parent->far_left = PGOFFSET(child_pgnum))
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


void adjust_root(page_t* root)
{
    pagenum_t root_pgnum = PGNUM(root->my_offset);

    // Case: nonempty root
    if(root->num_keys > 0)
    {
        file_write_page(root_pgnum, root);
        return;
    }

    // Case: empty root & it has child
    if(!root->is_leaf)
    {
        file_free_page(root_pgnum);
        header->rootpg_offset = root->far_left;
        page_t new_root;
        file_read_page(PGNUM(header->rootpg_offset), &new_root);
        new_root.parent = 0;
        file_write_page(PGNUM(header->rootpg_offset), &new_root);
        file_write_page(0, header);
    }
    
    // Case: empty root & no child
    else
    {
        file_free_page(root_pgnum);
        header->rootpg_offset = 0;
        file_write_page(0, header);
    }
}


// Merging node when negihbor has enough space to merge
void coalesce_nodes
(page_t* n, page_t* parent, page_t* neighbor, int neighbor_index, int64_t k_prime_key)
{
    pagenum_t node_pgnum = PGNUM(n->my_offset);
    pagenum_t parent_pgnum = PGNUM(parent->my_offset);
    pagenum_t neighbor_pgnum = PGNUM(neighbor->my_offset);
    int i, j;
    index_t* k_prime_index;
    // Swap neighbor with node if noid is on the far left
    if(neighbor_index == -2)
    {
        page_t* tmp;
        tmp = n;
        n = neighbor;
        neighbor = tmp;
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
        page_t child;
        for(i = 0; i < neighbor->num_keys; ++i)
        {
            pagenum_t tmp_pgnum = PGNUM(neighbor->index[i].offset);
            file_read_page(tmp_pgnum, &child);
            child.parent = PGOFFSET(neighbor_pgnum);
            file_write_page(tmp_pgnum, &child);
        }
    }

    file_write_page(neighbor_pgnum, neighbor);
    file_free_page(node_pgnum);
    free(k_prime_index);
    
    delete_entry(parent, k_prime_key);
}

// Redistribute when neighbor doesn't have enough space to merge
void redistribute_nodes
(page_t* n, page_t* parent, page_t* neighbor, int neighbor_index, int k_prime_index)
{
    pagenum_t node_pgnum = PGNUM(n->my_offset);
    pagenum_t parent_pgnum = PGNUM(parent->my_offset);
    pagenum_t neighbor_pgnum = PGNUM(neighbor->my_offset);
    pagenum_t k_prime_pgnum;
    index_t* k_prime;

    // Case: n is left-most child
    if(neighbor_index == -2)
    {
        k_prime_pgnum = PGNUM(neighbor->far_left);
        k_prime = make_index(parent->index[k_prime_index].key, k_prime_pgnum);
        // Move left-end key of the neighbor to parent
        memcpy(&parent->index[k_prime_index], &neighbor->index[0], sizeof(index_t));
        parent->index[k_prime_index].offset = PGOFFSET(neighbor_pgnum);
        // Make copy of the k_prime to left-end of the n
        memcpy(&n->index[0], k_prime, sizeof(index_t));
        n->num_keys++;
        free(k_prime);
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
        parent->index[k_prime_index].offset = PGOFFSET(node_pgnum);
        pagenum_t far_right = PGNUM(neighbor->index[neighbor->num_keys-1].offset);
        neighbor->num_keys--;
        // Move copy of the k_prime to the left-end of the n
        memcpy(&n->index[0], k_prime, sizeof(index_t));
        n->num_keys++;
        free(k_prime);
        // Connecting offset
        n->index[0].offset = n->far_left;
        n->far_left = PGOFFSET(far_right);
    }

    file_write_page(node_pgnum, n);
    file_write_page(parent_pgnum, parent);
    file_write_page(neighbor_pgnum, neighbor);
}

// Delete entry of the node
void delete_entry(page_t* node, int64_t key) 
{
    pagenum_t pgnum = PGNUM(node->my_offset);

    // First remove entry form node
    remove_entry_from_node(node, key);

    // Case: deletion frmo the root
    if(header->rootpg_offset == PGOFFSET(pgnum))
        return adjust_root(node);

    /* Case: deletion from a node below root */
    
    // Case: node stays at or above minimum
    if(node->num_keys >= min_keys)
    {
        file_write_page(pgnum, node);
        return;
    }

    /* Case: node falls below minimum */
    page_t parent;
    file_read_page(PGNUM(node->parent), &parent);

    int neighbor_index = get_neighbor_index(&parent, pgnum);
    int k_prime_index;
    if(neighbor_index == -2 || neighbor_index == -1)
        k_prime_index = 0;
    else
        k_prime_index = neighbor_index + 1;
    int64_t k_prime = parent.index[k_prime_index].key;
    pagenum_t neighbor_pgnum;
    switch(neighbor_index)
    {
        case -2:
            neighbor_pgnum = PGNUM(parent.index[0].offset);
            break;
        case -1:
            neighbor_pgnum = PGNUM(parent.far_left);
            break;
        default:
            neighbor_pgnum = PGNUM(parent.index[neighbor_index].offset);
            break;
    }

    page_t neighbor;
    file_read_page(neighbor_pgnum, &neighbor);

    int capacity = node->is_leaf ? leaf_order : internal_order - 1;

    // Case: merging
    if(neighbor.num_keys + node->num_keys < capacity)
        return coalesce_nodes(node, &parent, &neighbor, neighbor_index, k_prime);
    
    // Case: redistribution
    return redistribute_nodes(node, &parent, &neighbor, neighbor_index, k_prime_index); 
}



// Master deletion function
int delete(int64_t key)
{
    char* value;
    value = find(key);
    
    if(value == NULL)
    {
        printf("The value to key %ld doesn't exist.\n", key);
        return -1;
    }
 
    page_t leaf;
    pagenum_t leaf_pgnum = find_leaf(key);
    file_read_page(leaf_pgnum, &leaf);
    delete_entry(&leaf, key);
    
    free(value);
    return 0;
}

int open_db(char* pathname)
{
    // in case file exist
    if((fp = fopen(pathname, "r+b")) != NULL)
    {
        header = (page_t*)malloc(sizeof(page_t));
        file_read_page(0, header);
        return 0;
    }
    // in case file does not exist
    else
    {
        fp = fopen(pathname, "w+b");
        if(fp == NULL)
            return -1;
        printf("Creating new file.\n");
        header = (page_t*)malloc(sizeof(page_t));
        header->freepg_offset = 0;
        header->rootpg_offset = 0;
        header->num_pages = 1;
        header->my_offset = 0;
        file_write_page(0, header);
        file_alloc_page();
        return 0;
    }
    
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
                printf("%ld : %s  ", leaf->records[i].key, leaf->records[i].value);
        }
        printf("| ");
    }
    printf("\n");

    free(q->arr);
    free(q);
}
*/