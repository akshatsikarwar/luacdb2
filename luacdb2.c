#include <pthread.h>
#include <signal.h>
#include <string.h>

#include <lauxlib.h>
#include <lua.h>
#include <lualib.h>

#include <cdb2api.h>

#define MAX_PARAMS 32

typedef lua_State *Lua;

struct cdb2_async;

struct cdb2 {
    int running;
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

static void clear_params(struct cdb2 *cdb2)
{
    for (int i = 1; i <= cdb2->n_params; ++i) {
        free(cdb2->params[i]);
        cdb2->params[i] = NULL;
    }
    cdb2->n_params = 0;
    cdb2_clearbindings(cdb2->db);
}

static void *async_work(void *data)
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

static int comdb2_set_config(Lua L)
{
    cdb2_set_comdb2db_info(lua_tostring(L, 1));
    return 0;
}

static int __gc(Lua L)
{
    struct cdb2 *cdb2 = lua_touserdata(L, -1);
    if (cdb2->running || cdb2->async) {
        fprintf(stderr,  "closing active db handle\n");
    }
    if (cdb2->db) cdb2_close(cdb2->db);
    return 0;
}

static int async_stmt(Lua L)
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    if (cdb2->running || cdb2->async) {
        return luaL_error(L, "statement already active");
    }
    const char *sql = luaL_checkstring(L, 2);
    cdb2->running = 1;
    cdb2->async = calloc(1, sizeof(struct cdb2_async));
    cdb2->async->sql = strdup(sql);
    pthread_mutex_init(&cdb2->async->lock, NULL);
    pthread_cond_init(&cdb2->async->cond, NULL);
    if (pthread_create(&cdb2->async->thd, NULL, async_work, cdb2) != 0) {
        return luaL_error(L, "failed to create async statement");
    }
    return 0;
}


static int bind(Lua L)
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
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
    if (cdb2_column_value(cdb2->db, column) == NULL) {
        lua_pushnil(L);
        return 1;
    }
    switch (cdb2_column_type(cdb2->db, column)) {
    case CDB2_CSTRING: lua_pushstring(L, cdb2_column_value(cdb2->db, column)); break;
    case CDB2_INTEGER: lua_pushinteger(L, *(int64_t *)cdb2_column_value(cdb2->db, column)); break;
    case CDB2_REAL: lua_pushnumber(L, *(double *)cdb2_column_value(cdb2->db, column)); break;
    default: return luaL_error(L, "unsupported column type");
    }
    return 1;
}

static int drain(Lua L)
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    if (!cdb2->running) return luaL_error(L, "no active statement");
    if (cdb2->async && async_result(cdb2)) return luaL_error(L, cdb2_errstr(cdb2->db));
    int rc;
    while ((rc = cdb2_next_record(cdb2->db)) == CDB2_OK)
        ;
    if (rc != CDB2_OK_DONE) return luaL_error(L, cdb2_errstr(cdb2->db));
    cdb2->running = 0;
    return 0;
}

static int next_record(Lua L)
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    if (!cdb2->running) return luaL_error(L, "no active statement");
    if (cdb2->async && async_result(cdb2)) return luaL_error(L, cdb2_errstr(cdb2->db));
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

static int rd_stmt(Lua L)
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    if (cdb2->running || cdb2->async) {
        return luaL_error(L, "statement already active");
    }
    const char *sql = luaL_checkstring(L, 2);
    if (cdb2_run_statement(cdb2->db, sql) != 0) {
        return luaL_error(L, cdb2_errstr(cdb2->db));
    }
    clear_params(cdb2);
    cdb2->running = 1;
    return 0;
}

static int verify_err(Lua L)
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    if (!cdb2->running) return luaL_error(L, "no active statement");
    if (!cdb2->async) return luaL_error(L, "no async statement");
    int rc = async_result(cdb2);
    cdb2->running = 0;
    if (rc == 0) {
        while ((rc = cdb2_next_record(cdb2->db)) == CDB2_OK)
            ;
        if (rc == CDB2_OK_DONE) return 0;
        return luaL_error(L, cdb2_errstr(cdb2->db));
    } else if (rc != CDB2ERR_VERIFY_ERROR) {
        return luaL_error(L, cdb2_errstr(cdb2->db));
    }
    printf("%s rc:%d err:%s\n", __func__, rc, cdb2_errstr(cdb2->db));
    return 0;
}

static int wr_stmt(Lua L)
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    if (cdb2->running || cdb2->async) {
        return luaL_error(L, "statement already active");
    }
    const char *sql = luaL_checkstring(L, 2);
    if (cdb2_run_statement(cdb2->db, sql) != 0) {
        return luaL_error(L, cdb2_errstr(cdb2->db));
    }
    clear_params(cdb2);
    cdb2->running = 1;
    drain(L);
    return 0;
}


static void init_cdb2(Lua L)
{
    lua_pushcfunction(L, comdb2);
    lua_setglobal(L, "comdb2");

    lua_pushcfunction(L, comdb2_set_config);
    lua_setglobal(L, "comdb2_set_config");

    const struct luaL_Reg cdb2_funcs[] = {
        {"__gc", __gc},
        {"async_stmt", async_stmt},
        {"bind", bind},
        {"column_value", column_value},
        {"drain", drain},
        {"next_record", next_record},
        {"num_columns", num_columns},
        {"rd_stmt", rd_stmt},
        {"verify_err", verify_err},
        {"wr_stmt", wr_stmt},
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
    const char *config_file = getenv("CDB2_CONFIG");
    if (config_file) cdb2_set_comdb2db_config(config_file);
    signal(SIGPIPE, SIG_IGN);
    Lua L = luaL_newstate();
    luaL_openlibs(L);
    init_cdb2(L);
    luaL_loadfile(L, argc > 1 ? argv[1] : NULL);
    lua_newtable(L);
    for (int i = 1; i < argc; ++i) {
        lua_pushstring(L, argv[i]);
        lua_rawseti(L, -2, i - 1);
    }
    lua_setglobal(L, "arg");
    int rc = EXIT_SUCCESS;
    if (lua_pcall(L, 0, LUA_MULTRET, 0)) {
        fprintf(stderr, "%s\n", lua_tostring(L, -1));
        rc = EXIT_FAILURE;
    }
    lua_close(L);
    return rc;
}
