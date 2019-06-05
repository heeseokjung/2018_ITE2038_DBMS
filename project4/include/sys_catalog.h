#ifndef __SYS_CATALOG__
#define __SYS_CATALOG__

#include <stdint.h>
#include <stdio.h>

// The max number of tables
#define NUM_TABLES 10

typedef struct sys_catalog
{
    FILE* fp;
    char* table_name;
    int8_t tid;
    uint8_t max_opened;
    int64_t min_key;
    int64_t max_key;
    uint64_t num_column;
    uint64_t num_key;
    struct sys_catalog* next;
}sys_catalog;

// Header of the system catalog list
extern sys_catalog* cat_header;

// Pointer to system catalog list to fast access
extern sys_catalog** cat_pointer;

/* API */
void init_cat();
int mapping_cat(char* pathname, int num_column);
void reset_cat(int tid);
void shutdown_cat();

#endif