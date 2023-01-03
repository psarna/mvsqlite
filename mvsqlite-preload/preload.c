#define _GNU_SOURCE

#include "shim.h"
#include "stdio.h"

static void __attribute__((constructor)) preload_init(void) {
    fprintf(stderr, "INITIALIZING!\n");
    mvsqlite_global_init();
}
