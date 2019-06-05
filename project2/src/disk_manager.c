#include "disk_manager.h"
#include <string.h>

// Global FILE pointer initiallization
FILE* fp = NULL;

// Global header page
page_t* header;

pagenum_t file_alloc_page()
{
    pagenum_t ret_num;
    freepg_t freepg;
    file_read_page(0, header);

    // In case there exists free page
    if(header->freepg_offset != 0)
    {
        ret_num = PGNUM(header->freepg_offset);
        fseek(fp, header->freepg_offset, SEEK_SET);
        fread(&freepg, PGSIZE, 1, fp);
        header->freepg_offset = freepg.next_freepg;
        file_write_page(0, header);
        return ret_num;
    }
    // In case there is no free page
    else
    {
        header->freepg_offset = header->num_pages * PGSIZE;
        ret_num = PGNUM(header->freepg_offset);

        fseek(fp, header->freepg_offset, SEEK_SET);
        for(int i = 1; i < NUM_FREEPGS; ++i)
        {
            freepg.next_freepg = header->freepg_offset + i * PGSIZE;
            fwrite(&freepg, PGSIZE, 1, fp);
            fflush(fp);
        }
        freepg.next_freepg = 0;
        fwrite(&freepg, PGSIZE, 1, fp);
        fflush(fp);

        header->freepg_offset += PGSIZE;
        header->num_pages += NUM_FREEPGS;
        file_write_page(0, header);
        return ret_num;
    }
}

void file_free_page(pagenum_t pagenum)
{
    freepg_t freepg;
    file_read_page(0, header);

    freepg.next_freepg = header->freepg_offset;
    header->freepg_offset = PGOFFSET(pagenum);
    fseek(fp, PGOFFSET(pagenum), SEEK_SET);
    fwrite(&freepg, PGSIZE, 1, fp);
    fflush(fp);
    file_write_page(0, header);
}

void file_read_page(pagenum_t pagenum, page_t* dest)
{
    fseek(fp, PGOFFSET(pagenum), SEEK_SET);
    
    // In case header
    if(pagenum == 0)
    {
        header_t tmp_header;
        fread(&tmp_header, PGSIZE, 1, fp);
        dest->freepg_offset = tmp_header.freepg_offset;
        dest->rootpg_offset = tmp_header.rootpg_offset;
        dest->num_pages = tmp_header.num_pages;
        return;
    }

    /* In case internal or leaf */
    node_t tmp_pg;
    fread(&tmp_pg, PGSIZE, 1, fp);
    
    // In case leaf
    if(tmp_pg.is_leaf)
    {
        dest->parent = tmp_pg.parent;
        dest->is_leaf = tmp_pg.is_leaf;
        dest->num_keys = tmp_pg.num_keys;
        dest->sibling = tmp_pg.sibling;
        for(int i = 0; i < tmp_pg.num_keys; ++i)
            memcpy(&dest->record[i], &tmp_pg.record[i], sizeof(record_t));
        dest->my_offset = PGOFFSET(pagenum);
    }
    // In case internal
    else
    {
        dest->parent = tmp_pg.parent;
        dest->is_leaf = tmp_pg.is_leaf;
        dest->num_keys = tmp_pg.num_keys;
        dest->far_left = tmp_pg.far_left;
        for(int i = 0; i < tmp_pg.num_keys; ++i)
            memcpy(&dest->index[i], &tmp_pg.index[i], sizeof(index_t));
        dest->my_offset = PGOFFSET(pagenum); 
    }
}

void file_write_page(pagenum_t pagenum, const page_t* src)
{
    fseek(fp, PGOFFSET(pagenum), SEEK_SET);

    // In case header
    if(pagenum == 0)
    {
        header_t tmp_header;
        tmp_header.freepg_offset = src->freepg_offset;
        tmp_header.rootpg_offset = src->rootpg_offset;
        tmp_header.num_pages = src->num_pages;
        fwrite(&tmp_header, PGSIZE, 1, fp);
        fflush(fp);
        return;
    }

    // In case leaf
    if(src->is_leaf)
    {
        leaf_t leaf;
        leaf.parent = src->parent;
        leaf.is_leaf = src->is_leaf;
        leaf.num_keys = src->num_keys;
        leaf.sibling = src->sibling;
        for(int i = 0; i < src->num_keys; ++i)
            memcpy(&leaf.record[i], &src->record[i], sizeof(record_t));
        fwrite(&leaf, PGSIZE, 1, fp);
        fflush(fp);
    }
    // In case internal
    else
    {
        internal_t internal;
        internal.parent = src->parent;
        internal.is_leaf = src->is_leaf;
        internal.num_keys = src->num_keys;
        internal.far_left = src->far_left;
        for(int i = 0; i < src->num_keys; ++i)
            memcpy(&internal.index[i], &src->index[i], sizeof(index_t));
        fwrite(&internal, PGSIZE, 1, fp);
        fflush(fp);
    }
}
