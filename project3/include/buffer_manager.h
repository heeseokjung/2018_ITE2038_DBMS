#ifndef __BUFFER_MANAGER_H__
#define __BUFFER_MANAGER_H__

#include "disk_manager.h"

// Buffer pool entry
typedef struct buf_frame
{
    page_t* frame;
    int table_id;
    pagenum_t page_num;
    int is_dirty;
    int pin_count;
    union
    {
        struct buf_frame* prev;
        // Last page of the buffer pool(for LRU)
        struct buf_frame* last;
    };
    struct buf_frame* next;
}buf_frame;

// B+ tree header
extern buf_frame* header;

// Buffer pool header
extern buf_frame* buf_header;

// Buffer manager API
buf_frame* buf_get_page(int table_id, pagenum_t page_num);
void buf_put_page(buf_frame* buf, int is_dirty);
buf_frame* buf_alloc_page(int table_id, page_t* page);
void buf_free_page(buf_frame* buf);

#endif