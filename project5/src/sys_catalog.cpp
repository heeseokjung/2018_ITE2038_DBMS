#include "sys_catalog.h"
#include "buffer_manager.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

// Header of the system catalog list
sys_catalog* cat_header = NULL;

// Pointer to system catalog list to fast access
sys_catalog** cat_pointer = NULL;

// Initialize sys_catalog list on memory
void init_cat()
{
    cat_header = (sys_catalog*)malloc(sizeof(sys_catalog));
    cat_header->max_opened = 0;
    cat_header->next = NULL;
    for(int i = 1; i <= NUM_TABLES; ++i)
    {
        sys_catalog* tmp = (sys_catalog*)malloc(sizeof(sys_catalog));
        tmp->fp = NULL;
        tmp->table_name = NULL;
        tmp->tid = NUM_TABLES - i + 1;
        tmp->num_column = 0;
        tmp->num_key = 0;
        tmp->next = cat_header->next;
        cat_header->next = tmp;
    }

    cat_pointer = (sys_catalog**)malloc(sizeof(sys_catalog*) * (NUM_TABLES + 1));
    for(int i = 0; i <= NUM_TABLES; ++i)
        cat_pointer[i] = NULL;
}

// Mapping cat_pointer to sys_catalog list based of table id
int mapping_cat(char* pathname, int num_column)
{
    sys_catalog* tmp = cat_header->next;
    while(tmp != NULL)
    {
        if(tmp->table_name == NULL) {
            tmp = tmp->next;
            continue;
        }
        if(strcmp(pathname, tmp->table_name) == 0)
            break;
        tmp = tmp->next;
    }
    // Allocate new table_id
    if(tmp == NULL)
    {
        if(cat_header->max_opened == NUM_TABLES) {
            printf("sys_catalog: cannot make more than %d tables\n", NUM_TABLES);
            return -1;
        }
        else {
            tmp = cat_header->next;
            while(tmp != NULL) {
                if(tmp->table_name == NULL)
                    break;
                tmp = tmp->next;
            }
            assert(tmp != NULL);
            // If set the other variable for preprocessing,
            // must roll back in reset_cat!!!
            cat_header->max_opened++;
            tmp->table_name = (char*)malloc(sizeof(char) * 100);
            strcpy(tmp->table_name, pathname);
            tmp->num_column = num_column;
            cat_pointer[tmp->tid] = tmp;
            return tmp->tid;
        }
    }
    // Existing file
    else
        cat_pointer[tmp->tid] = tmp;
    return tmp->tid;
}

void reset_cat(int tid)
{
    sys_catalog* tmp = cat_header->next;
    while(tmp != NULL) {
        if(tmp->tid == tid)
            break;
        tmp = tmp->next;
    }
    assert(tmp != NULL);
    fclose(tmp->fp);
    tmp->fp = NULL;
    free(tmp->table_name);
    tmp->table_name = NULL;
    tmp->num_column = 0;
    tmp->num_key = 0;
    cat_header->max_opened--;
}

void shutdown_cat()
{
    free(cat_pointer);
    sys_catalog* tmp = cat_header->next;
    while(tmp != NULL) {
        cat_header->next = tmp->next;
        if(tmp->table_name != NULL)
            free(tmp->table_name);
        free(tmp);
        tmp = cat_header->next;
    }
    free(cat_header);
}
