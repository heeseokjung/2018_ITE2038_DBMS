#ifndef __DISK_MANGER_H__
#define __DISK_MANAGER_H__

#include <stdint.h>
#include <stdio.h>

// Size of 1 page
#define PGSIZE 4096
// The number of pages made by file_alloc_page for a time
#define NUM_FREEPGS 100

// Global FILE pointer
extern FILE* fp;

// Page number
typedef uint64_t pagenum_t;
// Page offset
typedef uint64_t pgoffset_t;

// Macro function to get page number
#define PGNUM(X) ((X) / (PGSIZE))
// Macro funtion to get page offset
#define PGOFFSET(X) ((X) * (PGSIZE))

/* On-disk page structures */

// Header page
typedef struct header_page
{
    uint64_t freepg_offset;
    uint64_t rootpg_offset;
    uint64_t num_pages;
    char reserved[4072];
}header_t;

// Free page
typedef struct free_page
{
    uint64_t next_freepg;
    char reserved[4088];
}freepg_t;

// Internal page
typedef struct index
{
    int64_t key;
    uint64_t offset;
}index_t;

typedef struct internal_page
{
    uint64_t parent;
    int32_t is_leaf;
    uint32_t num_keys;
    char reserved[104];
    uint64_t far_left;
    index_t index[248];
}internal_t;

// Leaf page
typedef struct record
{
    int64_t key;
    char value[120];
}record_t;

typedef struct leaf_page
{
    uint64_t parent;
    int32_t is_leaf;
    uint32_t num_keys;
    char reserved[104];
    uint64_t sibling;
    record_t record[31];
}leaf_t;

// Temporary page to read internal / leaf
typedef struct node
{
    uint64_t parent;
    int32_t is_leaf;
    uint32_t num_keys;
    char reserved[104];
    union
    {
        uint64_t far_left;
        uint64_t sibling;
    };
    union
    {
        index_t index[248];
        record_t record[31];
    };
}node_t;

// In-memory structure 
typedef struct page
{
    uint64_t freepg_offset;
    uint64_t rootpg_offset;
    uint64_t num_pages;
    // Leaf or interanl
    uint64_t parent;
    int32_t is_leaf;
    uint32_t num_keys;
    union
    {
        uint64_t far_left;
        uint64_t sibling;
    };
    // Current page offset
    uint64_t my_offset;
    union
    {
        index_t index[248];
        record_t record[31];
    }; 
}page_t;

// Global header page
extern page_t* header;

// File manger API
pagenum_t file_alloc_page();
void file_free_page(pagenum_t pagenum);
void file_read_page(pagenum_t pagenum, page_t* dest);
void file_write_page(pagenum_t pagenum, const page_t* src);

#endif