//test main

#include "bpt.h"
#include <unistd.h>

int main()
{
    if(open_db("out") < 0)
    {
        printf("Cannot open database.\n");
        exit(-1);
    }
    
    const int start = 1;
    const int end = 100000;
    
    printf("Insertion test. %d to %d\n", start, end);
    for(int i = start; i <= end; ++i)
    {
        if(insert(i, "test") < 0)
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
        ret = find(i);
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
        if(delete(i * 2) < 0)
        {
            printf("cannot delete.\n");
            exit(-1);
        }
        else
            printf("delete key : %d\n", i * 2);
    }

    for(int i = start; i <= end; ++i)
    {
        ret = find(i);
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
        if(insert(i*2, "after_delete") < 0)
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
        ret = find(i);
        if(ret == NULL)
        {
            printf("Cannot find key : %d\n", i);
            exit(-1);
        }
        else
            printf("find -> key : %d, value : %s\n", i, ret);
    }
}