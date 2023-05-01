#include <pthread.h>
#include <string.h>

#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#include <cdb2api.h>

#define MAX_PARAMS 32
#define SINGLE_ROW 1
#define WRITE_STMT 2

typedef lua_State *Lua;

struct cdb2_async;

struct cdb2 {
    int running;
    int is_single_row;
    int is_write_stmt;
    cdb2_hndl_tp *db;
    int n_params;
    void *params[32];
    struct cdb2_async *async;
};

struct cdb2_async {
    int done;
    int result;
    char *sql;
    pthread_mutex_t lock;
    pthread_cond_t cond;
    pthread_t thd;
};

static int run_statement(Lua);

static void check_run_statement_flags(Lua L, struct cdb2 *cdb2)
{
    cdb2->is_write_stmt = cdb2->is_single_row = 0;
    if (lua_gettop(L) == 3) {
        int val = lua_tointeger(L, 3);
        if (val == WRITE_STMT) {
            cdb2->is_write_stmt = 1;
        } else if (val == SINGLE_ROW) {
            cdb2->is_single_row = 1;
        } else {
            luaL_argerror(L, 3, "expected SINGLE_ROW/WRITE_STMT");
        }
    }
}

static void check_ok_done_for_single_row(Lua L, struct cdb2 *cdb2)
{
    if (!cdb2->running && !cdb2->is_single_row) return;
    if (cdb2_next_record(cdb2->db) != CDB2_OK_DONE) luaL_error(L, "unexpected row");
    cdb2->running = 0;
}

static int consume_write_stmt(Lua L, struct cdb2 *cdb2)
{
    int rc;
    if ((rc = cdb2_next_record(cdb2->db)) == CDB2_OK_DONE) return 0;
    if (rc != CDB2_OK) return luaL_error(L, cdb2_errstr(cdb2->db));
    if (cdb2_numcolumns(cdb2->db) != 1 ||
        (
            strcmp(cdb2_column_name(cdb2->db, 0), "version") != 0 &&
            strcmp(cdb2_column_name(cdb2->db, 0), "rows deleted") != 0 &&
            strcmp(cdb2_column_name(cdb2->db, 0), "rows inserted") != 0 &&
            strcmp(cdb2_column_name(cdb2->db, 0), "rows updated") != 0
        )
    ){
        char err[512];
        snprintf(err, sizeof(err), "unexpected response #cols:%d '%s'",
                cdb2_numcolumns(cdb2->db), cdb2_column_name(cdb2->db, 0));
        return luaL_error(L, err);
    }
    if (cdb2_next_record(cdb2->db) == CDB2_OK_DONE) return 0;
    return luaL_error(L, cdb2_errstr(cdb2->db));
}

static void clear_params(struct cdb2 *cdb2)
{
    for (int i = 1; i <= cdb2->n_params; ++i) {
        free(cdb2->params[i]);
        cdb2->params[i] = NULL;
    }
    cdb2->n_params = 0;
    cdb2_clearbindings(cdb2->db);
}

static void *run_async(void *data)
{
    struct cdb2 *cdb2 = data;
    struct cdb2_async *async = cdb2->async;
    async->result = cdb2_run_statement(cdb2->db, async->sql);
    clear_params(cdb2);
    pthread_mutex_lock(&async->lock);
    async->done = 1;
    pthread_cond_signal(&async->cond);
    pthread_mutex_unlock(&async->lock);
    return NULL;
}

static int async_result(struct cdb2 *cdb2)
{
    struct cdb2_async *async = cdb2->async;
    pthread_mutex_lock(&async->lock);
    if (!async->done) {
        pthread_cond_wait(&async->cond, &async->lock);
    }
    pthread_join(async->thd, NULL);
    pthread_mutex_unlock(&async->lock);
    pthread_mutex_destroy(&async->lock);
    pthread_cond_destroy(&async->cond);
    int rc = async->result;
    free(async->sql);
    free(cdb2->async);
    cdb2->async = NULL;
    return rc;
}

static int comdb2(Lua L)
{
    luaL_argcheck(L, lua_gettop(L) == 2, lua_gettop(L), "need: dbname tier");
    const char *dbname, *tier;
    if ((dbname = lua_tostring(L, 1)) == NULL) {
        return luaL_argerror(L, 1, "expected db-name");
    }
    if ((tier = lua_tostring(L, 2)) == NULL) {
        return luaL_argerror(L, 2, "expected db-tier");
    }
    cdb2_hndl_tp *db = NULL;
    if (cdb2_open(&db, dbname, tier, 0) != 0 || !db) {
        return luaL_error(L, cdb2_errstr(db));
    }
    struct cdb2 *cdb2 = lua_newuserdata(L, sizeof(struct cdb2));
    memset(cdb2, 0, sizeof(struct cdb2));
    cdb2->db = db;
    luaL_getmetatable(L, "cdb2");
    lua_setmetatable(L, -2);
    return 1;
}

static int __gc(Lua L)
{
    struct cdb2 *cdb2 = lua_touserdata(L, -1);
    check_ok_done_for_single_row(L, cdb2);
    if (cdb2->running || cdb2->async) {
        fprintf(stderr,  "closing active db handle\n");
    }
    cdb2_close(cdb2->db);
    return 0;
}

static int begin(Lua L)
{
    lua_pushstring(L, "begin");
    lua_pushinteger(L, WRITE_STMT);
    return run_statement(L);
}

static int bind(Lua L)
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    check_ok_done_for_single_row(L, cdb2);
    if (cdb2->running || cdb2->async) {
        return luaL_error(L, "statement already active");
    }
    luaL_argcheck(L, lua_gettop(L) == 3, lua_gettop(L), "need: index, value");
    int idx = lua_tointeger(L, 2);
    if (cdb2->params[idx]) return luaL_error(L, "parameter already bound");
    if (idx > cdb2->n_params) cdb2->n_params = idx;
    switch (lua_type(L, -1)) {
    case LUA_TNUMBER:
        if (lua_isinteger(L, -1)) {
            int64_t *val = cdb2->params[idx] = malloc(sizeof(int64_t));
            *val = lua_tointeger(L, -1);
            if (cdb2_bind_index(cdb2->db, idx, CDB2_INTEGER, val, sizeof(*val)) != 0) {
                return luaL_error(L, cdb2_errstr(cdb2->db));
            }
        } else if (lua_isnumber(L, -1)) {
            double *val = cdb2->params[idx] = malloc(sizeof(double));
            *val = lua_tonumber(L, -1);
            if (cdb2_bind_index(cdb2->db, idx, CDB2_REAL, val, sizeof(*val)) != 0) {
                return luaL_error(L, cdb2_errstr(cdb2->db));
            }
        }
        break;
    case LUA_TSTRING: {
            char *val = cdb2->params[idx] = strdup(lua_tostring(L, -1));
            if (cdb2_bind_index(cdb2->db, idx, CDB2_CSTRING, val, strlen(val)) != 0) {
                return luaL_error(L, cdb2_errstr(cdb2->db));
            }
        }
        break;
    default:
        return luaL_error(L, "unsupported parameter type");
    }
    return 0;
}

static int column_value(Lua L)
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    if (!cdb2->running) {
        return luaL_error(L, "no active statement");
    }
    int column = luaL_checkinteger(L, 2) - 1;
    switch (cdb2_column_type(cdb2->db, column)) {
    case CDB2_CSTRING: lua_pushstring(L, cdb2_column_value(cdb2->db, column)); break;
    case CDB2_INTEGER: lua_pushinteger(L, *(int64_t *)cdb2_column_value(cdb2->db, column)); break;
    case CDB2_REAL: lua_pushnumber(L, *(double *)cdb2_column_value(cdb2->db, column)); break;
    default: return luaL_error(L, "unsupported column type");
    }
    return 1;
}

static int commit(Lua L)
{
    lua_pushstring(L, "commit");
    lua_pushinteger(L, WRITE_STMT);
    return run_statement(L);
}

static int next_record(Lua L)
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    if (cdb2->async) {
        if (async_result(cdb2)) {
            luaL_error(L, cdb2_errstr(cdb2->db));
        }
    }
    if (!cdb2->running) {
        return luaL_error(L, "no active statement");
    }
    int rc = cdb2_next_record(cdb2->db);
    if (rc == CDB2_OK) {
        lua_pushboolean(L, 1);
    } else if (rc == CDB2_OK_DONE) {
        cdb2->running = 0;
        lua_pushboolean(L, 0);
    } else {
        return luaL_error(L, cdb2_errstr(cdb2->db));
    }
    return 1;
}

static int num_columns(Lua L)
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    if (!cdb2->running) {
        return luaL_error(L, "no active statement");
    }
    lua_pushinteger(L, cdb2_numcolumns(cdb2->db));
    return 1;
}

static int rollback(Lua L)
{
    lua_pushstring(L, "rollback");
    lua_pushinteger(L, WRITE_STMT);
    return run_statement(L);
}

static int run_statement(Lua L)
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    check_ok_done_for_single_row(L, cdb2);
    if (cdb2->running || cdb2->async) {
        return luaL_error(L, "statement already active");
    }
    const char *sql = luaL_checkstring(L, 2);
    check_run_statement_flags(L, cdb2);
    if (cdb2_run_statement(cdb2->db, sql) != 0) {
        return luaL_error(L, cdb2_errstr(cdb2->db));
    }
    clear_params(cdb2);
    if (cdb2->is_write_stmt) {
        consume_write_stmt(L, cdb2);
    } else {
        cdb2->running = 1;
    }
    return 0;
}

static int run_statement_async(Lua L)
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    check_ok_done_for_single_row(L, cdb2);
    if (cdb2->running || cdb2->async) {
        return luaL_error(L, "statement already active");
    }
    const char *sql = luaL_checkstring(L, 2);
    check_run_statement_flags(L, cdb2);
    cdb2->async = calloc(1, sizeof(struct cdb2_async));
    cdb2->async->sql = strdup(sql);
    pthread_mutex_init(&cdb2->async->lock, NULL);
    pthread_cond_init(&cdb2->async->cond, NULL);
    if (pthread_create(&cdb2->async->thd, NULL, run_async, cdb2) != 0) {
        return luaL_error(L, "failed to create async statement");
    }
    return 0;
}

static int set_isolation(Lua L)
{
    const char *sql = luaL_checkstring(L, 2);
    char isolation[128];
    snprintf(isolation, sizeof(isolation), "set transaction %s", sql);
    lua_pop(L, 1);
    lua_pushstring(L, isolation);
    lua_pushinteger(L, WRITE_STMT);
    return run_statement(L);
}

static int await(Lua L)
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    if (!cdb2->async) return luaL_error(L, "no active statement");
    if (!cdb2->is_write_stmt) return luaL_error(L, "no write statement");
    if (async_result(cdb2)) {
        luaL_error(L, cdb2_errstr(cdb2->db));
    }
    if (cdb2->is_write_stmt) {
        consume_write_stmt(L, cdb2);
    }
    return 0;
}

static void init_cdb2(Lua L)
{
    lua_pushinteger(L, SINGLE_ROW);
    lua_setglobal(L, "SINGLE_ROW");

    lua_pushinteger(L, WRITE_STMT);
    lua_setglobal(L, "WRITE_STMT");

    lua_pushcfunction(L, comdb2);
    lua_setglobal(L, "comdb2");

    const struct luaL_Reg cdb2_funcs[] = {
        {"__gc", __gc},
        {"begin", begin},
        {"bind", bind},
        {"column_value", column_value},
        {"commit", commit},
        {"next_record", next_record},
        {"num_columns", num_columns},
        {"rollback", rollback},
        {"run_statement", run_statement},
        {"run_statement_async", run_statement_async},
        {"set_isolation", set_isolation},
        {"await", await},
        {NULL, NULL}
    };
    luaL_newmetatable(L, "cdb2");
    lua_pushvalue(L, -1);
    lua_setfield(L, -2, "__index");
    luaL_setfuncs(L, cdb2_funcs, 0);
    lua_pop(L, 1);
}

int main(int argc, char **argv)
{
    Lua L = luaL_newstate();
    luaL_openlibs(L);
    init_cdb2(L);
    luaL_loadfile(L, argc > 1 ? argv[1] : NULL);
    int rc = EXIT_SUCCESS;
    if (lua_pcall(L, 0, LUA_MULTRET, 0)) {
        fprintf(stderr, "%s\n", lua_tostring(L, -1));
        rc = EXIT_FAILURE;
    }
    lua_close(L);
    return rc;
}
