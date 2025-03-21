#include <alloca.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/uio.h>
#include <unistd.h>
#include <uuid/uuid.h>

#include <lauxlib.h>
#include <lua.h>
#include <lualib.h>

#include <cdb2api.h>

#define MAX_PARAMS 32
#define have_active_stmt "have active statement"
#define no_active_stmt "no active statement"

typedef lua_State *Lua;

static uint8_t invalid_hex = 'x';
static uint8_t hex_map[255] = { 'x' };

static int die = 0;
#define luacdb2_error(...) ({ die = 1; luaL_error(__VA_ARGS__); })

struct cdb2 {
    char *dbname;
    char *tier;
    char *errstr;
    cdb2_hndl_tp *db;
    int n_params;
    void *param_value[MAX_PARAMS];
    void *param_name[MAX_PARAMS];

    int async;
    int done_run_stmt;
    int running; /* keep calling cdb2_next_record */
    int rc;
    char *sql;
    pthread_mutex_t lock;
    pthread_cond_t cond;
    pthread_t thd;
};

static void clear_params(struct cdb2 *cdb2)
{
    for (int i = 1; i <= cdb2->n_params; ++i) {
        if (cdb2->param_name[i]) {
            free(cdb2->param_name[i]);
            cdb2->param_name[i] = NULL;
        }
        free(cdb2->param_value[i]);
        cdb2->param_value[i] = NULL;
    }
    cdb2->n_params = 0;
    cdb2_clearbindings(cdb2->db);
}

static int cdb2(Lua L)
{
    int args = lua_gettop(L);
    if (args == 0) return luaL_argerror(L, 1, "dbname expected");
    if (args > 2) luaL_argerror(L, 3, "unexpected arguments");
    int flags = 0;

    char *dbname = strdup(luaL_checkstring(L, 1));
    char *tier = strdup(args != 2 ? "default" : luaL_checkstring(L, 2));
    if (tier[0] == '@') {
        if (strchr(tier, ',') == NULL) {
            flags |= CDB2_DIRECT_CPU;
            char *tmp = strdup(tier + 1);
            free(tier);
            tier = tmp;
        }
    }
    cdb2_hndl_tp *db = NULL;
    if (cdb2_open(&db, dbname, tier, flags) != 0 || !db) {
        return luacdb2_error(L, cdb2_errstr(db));
    }
    struct cdb2 *cdb2 = lua_newuserdata(L, sizeof(struct cdb2));
    memset(cdb2, 0, sizeof(struct cdb2));
    cdb2->dbname = dbname;
    cdb2->tier = tier;
    cdb2->db = db;
    luaL_getmetatable(L, "cdb2");
    lua_setmetatable(L, -2);
    return 1;
}
static void *async_worker(void * data)
{
    struct cdb2 *cdb2 = data;
    pthread_mutex_lock(&cdb2->lock);
    pthread_cond_signal(&cdb2->cond);
    pthread_mutex_unlock(&cdb2->lock);

    pthread_mutex_lock(&cdb2->lock);
    pthread_cond_wait(&cdb2->cond, &cdb2->lock);
    while (cdb2->async) {
        pthread_mutex_unlock(&cdb2->lock);
        cdb2->rc = cdb2_run_statement(cdb2->db, cdb2->sql);
        free(cdb2->sql);
        clear_params(cdb2);

        pthread_mutex_lock(&cdb2->lock);
        cdb2->done_run_stmt = 1;
        pthread_cond_signal(&cdb2->cond);
        pthread_mutex_unlock(&cdb2->lock);

        pthread_mutex_lock(&cdb2->lock);
        pthread_cond_wait(&cdb2->cond, &cdb2->lock);
    }
    pthread_mutex_unlock(&cdb2->lock);
    return NULL;
}

static int cdb2x(Lua L)
{
    cdb2(L);
    struct cdb2 *cdb2 = lua_touserdata(L, -1);
    cdb2->async = 1;
    pthread_mutex_init(&cdb2->lock, NULL);
    pthread_cond_init(&cdb2->cond, NULL);

    pthread_mutex_lock(&cdb2->lock);
    pthread_create(&cdb2->thd, NULL, async_worker, cdb2);
    pthread_cond_wait(&cdb2->cond, &cdb2->lock);
    pthread_mutex_unlock(&cdb2->lock);
    return 1;
}

static int comdb2db_info(Lua L)
{
    cdb2_set_comdb2db_info((char *)lua_tostring(L, 1));
    return 0;
}

static int disable_sockpool(Lua L)
{
    cdb2_disable_sockpool();
    return 0;
}

static void cdb2_wait(struct cdb2 *cdb2)
{
    if (cdb2->done_run_stmt) {
        return;
    }
    pthread_cond_wait(&cdb2->cond, &cdb2->lock);
    if (!cdb2->done_run_stmt) abort();
}

static void cdb2_dispatch(Lua L, struct cdb2 *cdb2, const char *sql)
{
    pthread_mutex_lock(&cdb2->lock);
    if (cdb2->running) {
        luacdb2_error(L, have_active_stmt);
    }
    cdb2->sql = strdup(sql);
    cdb2->running = 1;
    cdb2->done_run_stmt = 0;
    pthread_cond_signal(&cdb2->cond);
    pthread_mutex_unlock(&cdb2->lock);
}

static int __gc(Lua L)
{
    if (die) return 0;
    struct cdb2 *cdb2 = lua_touserdata(L, -1);
    if (cdb2->running) {
        fprintf(stderr,  "closing active statement\n");
    }
    if (cdb2->async) {
        pthread_mutex_lock(&cdb2->lock);
        cdb2_wait(cdb2);
        cdb2->async = 0;
        pthread_cond_signal(&cdb2->cond);
        pthread_mutex_unlock(&cdb2->lock);

        pthread_join(cdb2->thd, NULL);
        pthread_cond_destroy(&cdb2->cond);
        pthread_mutex_destroy(&cdb2->lock);
    }
    if (cdb2->db) {
        cdb2_close(cdb2->db);
        cdb2->db = NULL;
    }
    if (cdb2->dbname) {
        free(cdb2->dbname);
        cdb2->dbname = NULL;
    }
    if (cdb2->tier) {
        free(cdb2->tier);
        cdb2->tier = NULL;
    }
    if (cdb2->errstr) {
        free(cdb2->errstr);
        cdb2->errstr = NULL;
    }
    return 0;
}

static int bind_index(Lua L) /* 1-indexed */
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    if (cdb2->running) return luacdb2_error(L, have_active_stmt);
    int idx = lua_tointeger(L, 2);
    if (idx >= MAX_PARAMS) return luacdb2_error(L, "too many params");
    if (cdb2->param_value[idx]) return luacdb2_error(L, "parameter already bound");
    if (idx > cdb2->n_params) cdb2->n_params = idx;
    switch (lua_type(L, -1)) {
    case LUA_TNUMBER:
        if (lua_isinteger(L, -1)) {
            int64_t *val = cdb2->param_value[idx] = malloc(sizeof(int64_t));
            *val = lua_tointeger(L, -1);
            if (cdb2_bind_index(cdb2->db, idx, CDB2_INTEGER, val, sizeof(*val)) != 0) {
                return luacdb2_error(L, cdb2_errstr(cdb2->db));
            }
        } else if (lua_isnumber(L, -1)) {
            double *val = cdb2->param_value[idx] = malloc(sizeof(double));
            *val = lua_tonumber(L, -1);
            if (cdb2_bind_index(cdb2->db, idx, CDB2_REAL, val, sizeof(*val)) != 0) {
                return luacdb2_error(L, cdb2_errstr(cdb2->db));
            }
        }
        break;
    case LUA_TSTRING: {
            char *val = cdb2->param_value[idx] = strdup(lua_tostring(L, -1));
            if (cdb2_bind_index(cdb2->db, idx, CDB2_CSTRING, val, strlen(val)) != 0) {
                return luacdb2_error(L, cdb2_errstr(cdb2->db));
            }
        }
        break;
    case LUA_TNIL: {
            if (cdb2_bind_index(cdb2->db, idx, CDB2_CSTRING, NULL, 0) != 0) {
                return luacdb2_error(L, cdb2_errstr(cdb2->db));
            }
        }
        break;
    default:
        return luacdb2_error(L, "unsupported parameter type");
    }
    return 0;
}

static int bind_param(Lua L)
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    if (cdb2->running) return luacdb2_error(L, have_active_stmt);
    if (cdb2->n_params >= MAX_PARAMS) return luacdb2_error(L, "too many params");
    int idx = cdb2->n_params++;
    char *param = cdb2->param_name[idx] = strdup(lua_tostring(L, 2));
    switch (lua_type(L, -1)) {
    case LUA_TNUMBER:
        if (lua_isinteger(L, -1)) {
            int64_t *val = cdb2->param_value[idx] = malloc(sizeof(int64_t));
            *val = lua_tointeger(L, -1);
            if (cdb2_bind_param(cdb2->db, param, CDB2_INTEGER, val, sizeof(*val)) != 0) {
                return luacdb2_error(L, cdb2_errstr(cdb2->db));
            }
        } else if (lua_isnumber(L, -1)) {
            double *val = cdb2->param_value[idx] = malloc(sizeof(double));
            *val = lua_tonumber(L, -1);
            if (cdb2_bind_param(cdb2->db, param, CDB2_REAL, val, sizeof(*val)) != 0) {
                return luacdb2_error(L, cdb2_errstr(cdb2->db));
            }
        }
        break;
    case LUA_TSTRING: {
            char *val = cdb2->param_value[idx] = strdup(lua_tostring(L, -1));
            if (cdb2_bind_param(cdb2->db, param, CDB2_CSTRING, val, strlen(val)) != 0) {
                return luacdb2_error(L, cdb2_errstr(cdb2->db));
            }
        }
        break;
    case LUA_TNIL: {
            if (cdb2_bind_param(cdb2->db, param, CDB2_CSTRING, NULL, 0) != 0) {
                return luacdb2_error(L, cdb2_errstr(cdb2->db));
            }
        }
        break;
    default:
        return luacdb2_error(L, "unsupported parameter type");
    }
    return 0;
}

static int cdb2_bind(Lua L)
{
    luaL_argcheck(L, lua_gettop(L) == 3, lua_gettop(L), "need: index/name, value");
    if (lua_isinteger(L, 2)) return bind_index(L);
    return bind_param(L);
}

static void hex_init(void)
{
    for (int i = '0'; i <= '9'; ++i) hex_map[i] = i - '0';
    for (int i = 'A'; i <= 'F'; ++i) hex_map[i] = i - 'A' + 10;
    for (int i = 'a'; i <= 'f'; ++i) hex_map[i] = i - 'a' + 10;
}

static struct iovec hex_to_binary(Lua L, const char *str)
{
    size_t len = strlen(str);
    if (str[0] == 'x' && str[1] == '\'' && str[len - 1] == '\'') {
        str += 2;
        len -= 3;
    }
    struct iovec v;
    if (len == 0) {
        v.iov_base = malloc(0);
        v.iov_len = 0;
        return v;
    }
    if (len % 2) luacdb2_error(L, "bind_blob: bad hex string");
    v.iov_base = malloc(len / 2);
    v.iov_len = len / 2;
    uint8_t *b = v.iov_base;
    for (int i = 0; i < len; ++b) {
        uint8_t  first = hex_map[str[i++]];
        uint8_t second = hex_map[str[i++]];
        if (first == invalid_hex || second == invalid_hex) luacdb2_error(L, "bind_blob: bad hex string");
        *b = (first << 4) | second;
    }
    return v;
}

static int bind_index_blob(Lua L) /* 1-indexed */
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    if (cdb2->running) return luacdb2_error(L, have_active_stmt);
    int idx = lua_tointeger(L, 2);
    if (idx >= MAX_PARAMS) return luacdb2_error(L, "too many params");
    if (cdb2->param_value[idx]) return luacdb2_error(L, "parameter already bound");
    if (idx > cdb2->n_params) cdb2->n_params = idx;
    struct iovec blob = hex_to_binary(L, luaL_checkstring(L, 3));
    cdb2->param_value[idx] = blob.iov_base;
    if (cdb2_bind_index(cdb2->db, idx, CDB2_BLOB, blob.iov_base, blob.iov_len) != 0) {
        return luacdb2_error(L, cdb2_errstr(cdb2->db));
    }
    return 0;
}

static int bind_param_blob(Lua L)
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    if (cdb2->running) return luacdb2_error(L, have_active_stmt);
    if (cdb2->n_params >= MAX_PARAMS) return luacdb2_error(L, "too many params");
    int idx = cdb2->n_params++;
    char *param = cdb2->param_name[idx] = strdup(lua_tostring(L, 2));
    struct iovec blob = hex_to_binary(L, luaL_checkstring(L, 3));
    cdb2->param_value[idx] = blob.iov_base;
    if (cdb2_bind_param(cdb2->db, param, CDB2_BLOB, blob.iov_base, blob.iov_len) != 0) {
        return luacdb2_error(L, cdb2_errstr(cdb2->db));
    }
    return 0;
}

static int bind_blob(Lua L)
{
    luaL_argcheck(L, lua_gettop(L) == 3, lua_gettop(L), "need: index/name, value");
    if (lua_isinteger(L, 2)) return bind_index_blob(L);
    return bind_param_blob(L);
}

static int column_name(Lua L)
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    if (!cdb2->running) {
        return luacdb2_error(L, no_active_stmt);
    }
    int col = luaL_checkinteger(L, 2) - 1;
    lua_pushstring(L, cdb2_column_name(cdb2->db, col));
    return 1;
}

static int column_type(Lua L)
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    if (!cdb2->running) {
        return luacdb2_error(L, no_active_stmt);
    }
    int col = luaL_checkinteger(L, 2) - 1;
    switch (cdb2_column_type(cdb2->db, col)) {
    case CDB2_INTEGER: lua_pushstring(L, "integer"); return 1;
    case CDB2_CSTRING: lua_pushstring(L, "string"); return 1;
    case CDB2_REAL: lua_pushstring(L, "double"); return 1;
    case CDB2_BLOB: lua_pushstring(L, "blob"); return 1;
    default: return luacdb2_error(L, "unimplemented type:%d", cdb2_column_type(cdb2->db, col));
    }
}

static void binary_to_hex(Lua L, void *ptr, size_t len)
{
    char *hex, *h;
    hex = h = alloca(len * 2 + 3 + 1);
    *h++ = 'x';
    *h++ = '\'';
    char map[]={'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};
    for (int i = 0; i < len; ++i) {
        uint8_t byte = *((uint8_t *)ptr + i);
        *h++ = map[(byte & 0xf0) >> 4];
        *h++ = map[byte & 0x0f];
    }
    *h++ = '\'';
    *h++ = 0;
    lua_pushstring(L, hex);
}

static void push_datetime(Lua L, cdb2_client_datetime_t *dt)
{
    char buf[256];
    sprintf(buf, "%4.4u-%2.2u-%2.2uT%2.2u%2.2u%2.2u.%3.3u %s",
            dt->tm.tm_year + 1900, dt->tm.tm_mon + 1, dt->tm.tm_mday,
            dt->tm.tm_hour, dt->tm.tm_min, dt->tm.tm_sec, dt->msec,
            dt->tzname);
    lua_pushstring(L, buf);
}

static void push_datetimeus(Lua L, cdb2_client_datetimeus_t *dt)
{
    char buf[256];
    sprintf(buf, "%4.4u-%2.2u-%2.2uT%2.2u%2.2u%2.2u.%3.6u %s",
            dt->tm.tm_year + 1900, dt->tm.tm_mon + 1, dt->tm.tm_mday,
            dt->tm.tm_hour, dt->tm.tm_min, dt->tm.tm_sec, dt->usec,
            dt->tzname);
    lua_pushstring(L, buf);
}

static int column_value(Lua L)
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    if (!cdb2->running) {
        return luacdb2_error(L, no_active_stmt);
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
    case CDB2_BLOB: binary_to_hex(L, cdb2_column_value(cdb2->db, column), cdb2_column_size(cdb2->db, column)); break;
    case CDB2_DATETIME: push_datetime(L, cdb2_column_value(cdb2->db, column)); break;
    case CDB2_DATETIMEUS: push_datetimeus(L, cdb2_column_value(cdb2->db, column)); break;
    default: return luacdb2_error(L, "unsupported column type for '%s'", cdb2_column_name(cdb2->db, column));
    }
    return 1;
}

static int drain(Lua L)
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    if (!cdb2->running) return luacdb2_error(L, no_active_stmt);
    if (cdb2->async) {
        pthread_mutex_lock(&cdb2->lock);
        cdb2_wait(cdb2);
        pthread_mutex_unlock(&cdb2->lock);
        if (cdb2->rc != 0) {
            return luacdb2_error(L, "async cdb2_run_statement rc:%d err:%s", cdb2->rc, cdb2_errstr(cdb2->db));
        }
    }
    int rc;
    while ((rc = cdb2_next_record(cdb2->db)) == CDB2_OK)
        ;
    if (rc != CDB2_OK_DONE) return luacdb2_error(L, "rc:%d err:%s", rc, cdb2_errstr(cdb2->db));
    cdb2->running = 0;
    return 0;
}

static int get_effects(Lua L)
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    if (cdb2->running) return luacdb2_error(L, have_active_stmt);
    cdb2_effects_tp e;
    cdb2_get_effects(cdb2->db, &e);

    lua_newtable(L);
    lua_pushstring(L, "num_inserted");
    lua_pushinteger(L, e.num_inserted);
    lua_settable(L, -3);
    lua_pushstring(L, "num_updated");
    lua_pushinteger(L, e.num_updated);
    lua_settable(L, -3);
    lua_pushstring(L, "num_deleted");
    lua_pushinteger(L, e.num_deleted);
    lua_settable(L, -3);
    return 1;
}

static int last_err(Lua L)
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    if (cdb2->running) return luacdb2_error(L, have_active_stmt);
    lua_pushstring(L, cdb2->errstr);
    return 1;
}

static int next_record(Lua L)
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    if (!cdb2->running) return luacdb2_error(L, no_active_stmt);
    if (cdb2->async) {
        pthread_mutex_lock(&cdb2->lock);
        cdb2_wait(cdb2);
        pthread_mutex_unlock(&cdb2->lock);
        if (cdb2->rc != 0) {
            return luacdb2_error(L, "async cdb2_run_statement rc:%d err:%s", cdb2->rc, cdb2_errstr(cdb2->db));
        }
    }
    int rc = cdb2_next_record(cdb2->db);
    if (rc == CDB2_OK) {
        lua_pushboolean(L, 1);
    } else if (rc == CDB2_OK_DONE) {
        cdb2->running = 0;
        lua_pushboolean(L, 0);
    } else {
        cdb2->running = 0;
        return luacdb2_error(L, cdb2_errstr(cdb2->db));
    }
    return 1;
}

static int num_columns(Lua L)
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    if (!cdb2->running) {
        return luacdb2_error(L, no_active_stmt);
    }
    lua_pushinteger(L, cdb2_numcolumns(cdb2->db));
    return 1;
}

static int rd_stmt_int(Lua L, int fail)
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    if (cdb2->running) {
        return luacdb2_error(L, have_active_stmt);
    }
    const char *sql = luaL_checkstring(L, 2);
    if (cdb2->async) {
        cdb2_dispatch(L, cdb2, sql);
        lua_pushboolean(L, 1);
        return 1;
    }
    if (cdb2_run_statement(cdb2->db, sql) != 0) {
        if (fail) return luacdb2_error(L, cdb2_errstr(cdb2->db));
        fprintf(stderr, "%s\n", cdb2_errstr(cdb2->db));
        lua_pushboolean(L, 0);
        return 1;
    }
    clear_params(cdb2);
    cdb2->running = 1;
    if (fail) return 0;
    lua_pushboolean(L, 1);
    return 1;
}

static int rd_stmt(Lua L)
{
    return rd_stmt_int(L, 1);
}

static int run_statement(Lua L)
{
    return rd_stmt_int(L, 1);
}

static int try_rd_stmt(Lua L)
{
    return rd_stmt_int(L, 0);
}

static int expect_err(Lua L, int expected)
{
    int rc;
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    const char *sql = luaL_checkstring(L, 2);
    if (cdb2->running) {
            return luacdb2_error(L, have_active_stmt);
    }
    rc = cdb2_run_statement(cdb2->db, sql);
    clear_params(cdb2);
    if (rc == 0) {
        while ((rc = cdb2_next_record(cdb2->db)) == CDB2_OK)
            ;
        if (rc == CDB2_OK_DONE) {
            lua_pushboolean(L, 0);
            return 1;
        }
    }
    if (rc != expected) {
        return luacdb2_error(L, "expected:%d rc:%d err:%s", expected, rc, cdb2_errstr(cdb2->db));
    }
    free(cdb2->errstr);
    cdb2->errstr = strdup(cdb2_errstr(cdb2->db));
    cdb2->running = 0;
    cdb2_close(cdb2->db);
    cdb2_open(&cdb2->db, cdb2->dbname, cdb2->tier, 0);
    lua_pushboolean(L, 1);
    return 1;
}

static int duplicate_err(Lua L)
{
    return expect_err(L, CDB2ERR_DUPLICATE);
}

static int verify_err(Lua L)
{
    return expect_err(L, CDB2ERR_VERIFY_ERROR);
}

static int querylimit_err(Lua L)
{
    return expect_err(L, CDB2ERR_QUERYLIMIT);
}

static int readonly_err(Lua L)
{
    return expect_err(L, CDB2ERR_READONLY);
}

static int wr_stmt(Lua L)
{
    struct cdb2 *cdb2 = luaL_checkudata(L, 1, "cdb2");
    if (cdb2->running) {
        return luacdb2_error(L, have_active_stmt);
    }
    const char *sql = luaL_checkstring(L, 2);
    if (cdb2->async) {
        cdb2_dispatch(L, cdb2, sql);
        return 0;
    }
    int rc = cdb2_run_statement(cdb2->db, sql);
    if (rc) {
        return luacdb2_error(L, "rc:%d err:%s", rc, cdb2_errstr(cdb2->db));
    }
    clear_params(cdb2);
    cdb2->running = 1;
    drain(L);
    return 0;
}

static void get_timeval(Lua L, int idx, struct timeval *t)
{
    luaL_checktype(L, idx, LUA_TTABLE);

    lua_pushstring(L, "sec");
    lua_gettable(L, idx);
    t->tv_sec = lua_tointeger(L, -1);
    lua_pop(L, 1);

    lua_pushstring(L, "usec");
    lua_gettable(L, idx);
    t->tv_usec = lua_tointeger(L, -1);
    lua_pop(L, 1);
}

static void push_timeval(Lua L, struct timeval *t)
{
    lua_newtable(L);
    lua_pushstring(L, "sec");
    lua_pushnumber(L, t->tv_sec);
    lua_settable(L, -3);
    lua_pushstring(L, "usec");
    lua_pushnumber(L, t->tv_usec);
    lua_settable(L, -3);
}

static int luacdb2_gettimeofday(Lua L)
{
    struct timeval t;
    gettimeofday(&t, NULL);
    push_timeval(L, &t);
    return 1;
}

static int luacdb2_timeradd(Lua L)
{
    struct timeval first, second, sum;
    get_timeval(L, 1, &first);
    get_timeval(L, 2, &second);
    timeradd(&first, &second, &sum);
    push_timeval(L, &sum);
    return 1;
}

static int luacdb2_timersub(Lua L)
{
    struct timeval start, end, diff;
    get_timeval(L, 1, &end);
    get_timeval(L, 2, &start);
    timersub(&end, &start, &diff);
    push_timeval(L, &diff);
    return 1;
}

static int luacdb2_sleep(Lua L)
{
    int sec = luaL_checkinteger(L, 1);
    sleep(sec);
    return 0;
}

static int luacdb2_sleepms(Lua L)
{
    int ms = luaL_checkinteger(L, 1);
    if (ms) poll(NULL, 0, ms);
    return 0;
}

static int luacdb2_setenv(Lua L)
{
    const char *key = luaL_checkstring(L, 1);
    const char *val = luaL_checkstring(L, 2);
    setenv(key, val, 1);
    return 0;
}

static int guid(Lua L)
{
    uuid_t id;
    char id_str[37];
    uuid_generate(id);
    uuid_unparse_lower(id, id_str);
    lua_pushstring(L, id_str);
    return 1;
}

static void init_cdb2(Lua L)
{
    hex_init();

    lua_pushcfunction(L, cdb2);
    lua_setglobal(L, "cdb2");

    lua_pushcfunction(L, cdb2x);
    lua_setglobal(L, "cdb2x");

    lua_pushcfunction(L, comdb2db_info);
    lua_setglobal(L, "comdb2db_info");

    lua_pushcfunction(L, disable_sockpool);
    lua_setglobal(L, "disable_sockpool");

    lua_pushcfunction(L, luacdb2_gettimeofday);
    lua_setglobal(L, "gettimeofday");

    lua_pushcfunction(L, luacdb2_timeradd);
    lua_setglobal(L, "timeradd");

    lua_pushcfunction(L, luacdb2_timersub);
    lua_setglobal(L, "timersub");

    lua_pushcfunction(L, luacdb2_sleep);
    lua_setglobal(L, "sleep");

    lua_pushcfunction(L, luacdb2_sleepms);
    lua_setglobal(L, "sleepms");

    lua_pushcfunction(L, luacdb2_setenv);
    lua_setglobal(L, "setenv");

    lua_pushcfunction(L, guid);
    lua_setglobal(L, "guid");

    const struct luaL_Reg cdb2_funcs[] = {
        {"__gc", __gc},
        {"bind", cdb2_bind},
        {"bind_blob", bind_blob},
        {"close", __gc},
        {"column_name", column_name},
        {"column_type", column_type},
        {"column_value", column_value},
        {"drain", drain},
        {"duplicate_err", duplicate_err},
        {"get_effects", get_effects},
        {"last_err", last_err},
        {"next_record", next_record},
        {"num_columns", num_columns},
        {"querylimit_err", querylimit_err},
        {"readonly_err", readonly_err},
        {"rd_stmt", rd_stmt},
        {"run_statement", run_statement},
        {"try_rd_stmt", try_rd_stmt},
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
    char *config_file = getenv("CDB2_CONFIG");
    if (config_file) cdb2_set_comdb2db_config(config_file);
    signal(SIGPIPE, SIG_IGN);
    Lua L = luaL_newstate();
    luaL_openlibs(L);
    init_cdb2(L);
    lua_newtable(L);
    for (int i = 1; i < argc; ++i) {
        lua_pushstring(L, argv[i]);
        lua_rawseti(L, -2, i - 1);
    }
    lua_setglobal(L, "argv");
    int rc = luaL_dofile(L, argc > 1 ? argv[1] : NULL);
    if (rc) {
        fprintf(stderr, "%s\n", lua_tostring(L, 1));
    }
    lua_close(L);
    return rc;
}
