#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1

#include "stdio.h"

extern void mwal_init();
extern struct libsql_wal_methods* mwal_methods();

int sqlite3_mwal_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
) {
  // yes, racy
  static int initialized = 0;
  if (initialized == 0) {
    initialized = 1;
  } else {
    return 0;
  }

  SQLITE_EXTENSION_INIT2(pApi);

  mwal_init();
  struct libsql_wal_methods *methods = mwal_methods();

  if (methods) {
    fprintf(stderr, "Found mwal methods, registering\n");
    int rc = libsql_wal_methods_register(methods);
    return rc == SQLITE_OK ? SQLITE_OK_LOAD_PERMANENTLY : rc;
  }
  // It's not fatal to fail to instantiate methods - it will be logged.
  return SQLITE_OK_LOAD_PERMANENTLY;
}
