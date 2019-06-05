#include "buffer_manager.h"
#include <stdlib.h>
#include <assert.h>

// B+ tree header
buf_frame* header = NULL;

// Buffer pool header
buf_frame* buf_header = NULL;

// Buffer pool latch
pthread_mutex_t bufcache_latch = PTHREAD_MUTEX_INITIALIZER;

buf_frame* buf_get_page(int table_id, pagenum_t page_num)
{
    buf_frame* buf = buf_header->next;
    while(buf != NULL)
    {
        //debug
        //printf("buf->table_id = %d / buf->page_num = %ld / buf address = %p\n", buf->table_id, buf->page_num, buf);
        if(buf->table_id == table_id && buf->page_num == page_num)
        {
            buf->pin_count++;
            return buf;
        }
        buf = buf->next;
    }

    // Case: cache miss occured
    if(buf == NULL)
    {
        buf = buf_header->last;
        while(buf != buf_header)
        {
            if(buf->pin_count == 0 && buf->is_dirty == 0)
            {
                buf->pin_count++;
                if(buf->is_dirty)
                {
                    file_write_page(buf->table_id, buf->page_num, buf->frame);
                    free(buf->frame);
                    buf->is_dirty = 0;
                }
                buf->frame = (page_t*)malloc(sizeof(page_t));
                file_read_page(table_id, page_num, buf->frame);
                buf->table_id = table_id;
                buf->page_num = page_num;
                //buf->is_dirty = 1;
                return buf;
            }
            buf = buf->prev;
        }
        assert(buf != buf_header);
    }
}

void buf_put_page(buf_frame* buf, int is_dirty)
{
    if(is_dirty)
        buf->is_dirty = 1;
    // Move page next to header
    buf->prev->next = buf->next;
    if(buf->next != NULL)
        buf->next->prev = buf->prev;
    else
        buf_header->last = buf->prev;
    buf->next = buf_header->next;
    buf->next->prev = buf;
    buf->prev = buf_header;
    buf_header->next = buf;
    // Unpin
    buf->pin_count--;
    buf = NULL;
}

buf_frame* buf_alloc_page(int table_id, page_t* page)
{
    buf_frame* buf = buf_header->next;
    header = buf_get_page(table_id, 0);
    pagenum_t freepg_num = file_alloc_page(table_id, header->frame);
    buf_put_page(header, 1);
    
    while(buf != NULL)
    {
        if(buf->pin_count == 0 && buf->frame == NULL)
        {
            buf->pin_count++;
            buf->frame = page;
            buf->frame->my_offset = PGOFFSET(freepg_num);
            buf->table_id = table_id;
            buf->page_num = freepg_num;
            buf->is_dirty = 1;
            return buf;
        }
        buf = buf->next;
    }
    if(buf == NULL)
    {
        buf = buf_header->last;
        while(buf != buf_header)
        {
            if(buf->pin_count == 0)
            {
                buf->pin_count++;
                if(buf->is_dirty)
                {
                    file_write_page(buf->table_id, buf->page_num, buf->frame);
                    free(buf->frame);
                }
                buf->frame = page;
                buf->frame->my_offset = PGOFFSET(freepg_num);
                buf->table_id = table_id;
                buf->page_num = freepg_num;
                buf->is_dirty = 1;
                return buf;
            }
            buf = buf->prev;
        }
        assert(buf != buf_header);
    }
}

void buf_free_page(buf_frame* buf)
{
    header = buf_get_page(buf->table_id, 0);
    file_free_page(buf->table_id, buf->page_num, header->frame);
    buf_put_page(header, 1);
    free(buf->frame);
    buf->frame = NULL;
    buf->table_id = -1;
    buf->page_num = -1;
    buf->is_dirty = 0;
    buf->pin_count = 0;
}