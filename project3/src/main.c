//test main

#include "bpt.h"
#include <assert.h>
#include <unistd.h>

int main()
{
    
    init_db(100);
    int tid;
    if((tid = open_table("out")) < 0)
    {
        printf("Cannot open database.\n");
        exit(-1);
    }
    
    
    const int start = 1;
    const int end = 100000;
    /*
    init_db(100);
    int t1, t2;
    t1 = open_table("hi");
    t2 = open_table("hello");

    for(int i = 1; i <= 100; ++i)
    {
        insert(t1, i, "hi");
        insert(t2, i, "hello");
    }

    close_table(t2);
    for(int i = 1; i <= 50; ++i)
        delete(t1, i * 2);
    t2 = open_table("hello");

    for(int i = 1; i <= 100; ++i)
    {
        char* ret1 = find(t1, i);
        if(ret1 == NULL)
            printf("cannot find hi %d\n", i);
        else
            printf("In file hi -> %d %s\n", i, ret1);
        char* ret2 = find(t2, i);
        assert(ret2 != NULL);
        printf("In file hello -> %d %s\n", i, ret2);
    }

    printf("shutdown = %d\n", shutdown_db());
    */
    /*
    for(int i = start; i <= end; ++i)
        insert(1, i, "test");
    close_table(1);
    open_table("out");
    print_tree();
    */
    
    
    printf("Insertion test. %d to %d\n", start, end);
    for(int i = start; i <= end; ++i)
    {
        if(insert(tid, i, "test") < 0)
        {
            printf("duplicate key\n");
            exit(-1);
        }
        else
            printf("key %d inserted.\n", i);
    }

    printf("Insertion succeed.\n");
    printf("Waiting...");
    sleep(5);

    printf("Find test. %d to %d\n", start, end);
    char* ret;
    for(int i = start; i <= end; ++i)
    {
        ret = find(tid, i);
        if(ret == NULL)
        {
            printf("Cannot find key : %d\n", i);
            exit(-1);
        }
        else
            printf("find -> key : %d, value : %s\n", i, ret);
    }
    
    printf("Find succeed.\n");
    printf("Waiting...\n");
    sleep(5);

    printf("Delete test.\n");
    for(int i = start; i <= end / 2; ++i)
    {
        if(delete(tid, i * 2) < 0)
        {
            printf("cannot delete.\n");
            exit(-1);
        }
        else
            printf("delete key : %d\n", i * 2);
    }

    for(int i = start; i <= end; ++i)
    {
        ret = find(tid, i);
        if(ret == NULL && i % 2 == 0)
            printf("Cannot find key : %d\n", i);
        else if(ret != NULL && i % 2 != 0)
            printf("find -> key : %d, value : %s\n", i, ret);
        else
        {
            printf("find after delete failed.\n");
            exit(-1);
        }
    }

    sleep(5);

    for(int i = start; i <= end / 2; ++i)
    {
        if(insert(tid, i*2, "after_delete") < 0)
        {
            printf("cannot insert after delete %d.\n", i);
            exit(-1);
        }
        else
            printf("%d inserted\n", i);
    }

    printf("find after delete\n");
    sleep(5);
    printf("Find test. %d to %d\n", start, end);

    for(int i = start; i <= end; ++i)
    {
        ret = find(tid, i);
        if(ret == NULL)
        {
            printf("Cannot find key : %d\n", i);
            exit(-1);
        }
        else
            printf("find -> key : %d, value : %s\n", i, ret);
    }
    
    printf("shutdown = %d\n", shutdown_db());
}