/*
 * Hash table is represented as `htable` array of size N (N = number of lines in
 * dict), where each element either points to the end of a singly-linked list or
 * is equal to zero. Lists are stored in pre-allocated array `clist` of size N.
 *
 * This implementation is memory-efficient and cache-friendly, requiring only
 * 12N + O(1) bytes of "real" memory which can be smaller than the size of
 * dict, sacrificing however for request processing speed, which is
 * O(req_size) in most cases, but may be up to O(dict_size) due to hash collision.
 */

#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

/* 2 additional bytes for '\n' and '\0' */
#define MAX_REQUEST_SIZE (128 * 1024 * 1024 + 2)

struct htable_entry {
    uint32_t ptr;
};

struct collision_list {
    uint32_t fpos;
    uint32_t len;
    uint32_t prev;
};

struct compr_collision_list {
    uint32_t fpos;
    uint32_t len;
};

typedef struct htable_entry htable_entry_t;
typedef struct collision_list collision_list_t;
typedef struct compr_collision_list compr_collision_list_t;

static char *dict;
static htable_entry_t *htable;
static collision_list_t *clist;
static compr_collision_list_t *cclist;
static size_t num_of_bytes, num_of_lines;
static size_t num_of_buckets;
static int clist_size = 1;

static uint32_t hash(char *s, size_t len)
{
    uint32_t hash = 5381;
    for (size_t i = 0; i < len; i++)
        hash = ((hash << 5) + hash) + (s[i] - 32);
    return hash % num_of_buckets;
}

static void htable_insert(size_t fpos, size_t len)
{
    uint32_t h = hash(dict + fpos, len);
    clist[clist_size].fpos = fpos;
    clist[clist_size].len = len;
    clist[clist_size].prev = htable[h].ptr;
    htable[h].ptr = clist_size;
    ++clist_size;
}

static bool htable_lookup(char *s, size_t len)
{
    uint32_t h = hash(s, len);
    uint32_t ptr = htable[h].ptr;
    uint32_t nextptr = htable[h + 1].ptr;
    for (; ptr < nextptr; ptr++) {
        if ((len == cclist[ptr].len) &&
            !memcmp(s, dict + cclist[ptr].fpos, len))
            return true;
    }
    return false;
}

int main(int argc, char **argv)
{
    if (argc != 2) {
        printf("usage: %s dictionary_file\n", argv[0]);
        return 2;
    }

    /* Map dictionary file directly to memory for easier access from program */
    int dictfd;
    if ((dictfd = open(argv[1], O_RDONLY)) < 0)
        return 1;

    struct stat st;
    fstat(dictfd, &st);
    num_of_bytes = st.st_size;
    if ((dict = mmap(0, num_of_bytes, PROT_READ, MAP_PRIVATE, dictfd, 0)) ==
        MAP_FAILED)
        return 1;

    /* Count number of lines in file */
    for (size_t i = 0; i < num_of_bytes; i++) {
        if (dict[i] == '\n')
            num_of_lines++;
    }
    num_of_lines++;

    num_of_buckets = num_of_lines;
    htable = calloc(num_of_buckets + 1, sizeof(htable_entry_t));
    clist = malloc((num_of_lines + 1) * sizeof(collision_list_t));

    size_t prev_start = 0;
    for (size_t i = 0; i < num_of_bytes; i++) {
        if (dict[i] == '\n') {
            htable_insert(prev_start, i - prev_start + 1);
            prev_start = i + 1;
        }
    }

    /* Compress collision list and update ptrs in htable accordingly */
    htable[num_of_buckets].ptr = num_of_lines;
    cclist = malloc((num_of_lines + 1) * sizeof(compr_collision_list_t));
    size_t clines = 0;
    for (size_t i = 0; i < num_of_buckets; i++) {
        uint32_t ptr = htable[i].ptr;
        htable[i].ptr = clines;
        while (ptr) {
            cclist[clines].fpos = clist[ptr].fpos;
            cclist[clines].len = clist[ptr].len;
            ptr = clist[ptr].prev;
            clines++;
        }
    }
    free(clist);

    /* Ready to accept requests */
    char *req_buf = malloc(MAX_REQUEST_SIZE);

    while (!feof(stdin)) {
        fgets(req_buf, MAX_REQUEST_SIZE, stdin);
        size_t len = strlen(req_buf);
        /* Exit on "exit" command */
        if (!strncmp(req_buf, "exit\n", len))
            break;
        printf("%s: %s\n", req_buf, htable_lookup(req_buf, len) ? "YES" : "NO");
    }

    free(req_buf);
    free(htable);
    free(cclist);
    munmap(dict, num_of_bytes);
    close(dictfd);
    return 0;
}