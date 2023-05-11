#!/home/asikarw1/src/luacdb2/build/luacdb2

function connect()
    local dbname = "akdb"
    local tier = "local"
    if #arg >= 1 then dbname = arg[1] end
    if #arg >= 2 then tier = arg[2] end
    for i = 1, thds do
        table.insert(dbs, comdb2(dbname, tier))
        db = dbs[#dbs]
        db:rd_stmt("select comdb2_dbname(), comdb2_host(), comdb2_semver()")
        print(string.format("thd:%2d   dbname:%s/%s   host:%s   version:%s", i, db:column_value(1), tier, db:column_value(2), db:column_value(3)))
        db:drain()
    end
end

function truncate()
    print('truncate')
    db:wr_stmt("drop table if exists t")
    --db:wr_stmt("create table if not exists t(i integer unique)")
    db:wr_stmt("create table if not exists t(i integer unique, j integer unique, k integer unique)")
    --db:wr_stmt("truncate table t")
end

function tbl_stats()
    db:wr_stmt("put tunable parallel_count 1")
    db:rd_stmt("select min(i), max(i), count(i) from t")
    print("min:".. db:column_value(1) .. " max:"..  db:column_value(2) .. " count:" .. db:column_value(3))
    db:drain()
end

function insert(total)
    print('insert')
    if total < thds then error("not enough data to insert") end

    if total < 100000 then
        db:wr_stmt("set transaction chunk 10000")
        db:wr_stmt("begin")
        db:wr_stmt(string.format("insert into t(i) select value from generate_series(1, %d)", total))
        db:wr_stmt("commit")
        --for i = 1, total, 10000 do
        --    local ins = string.format("insert into t(i) select value from generate_series(%d, %d)", i, i + 10000 - 1)
        --    print(ins)
        --    db:wr_stmt(ins)
        --end
        return
    end

    if math.fmod(total, thds) ~= 0 then error("need count divisible by " .. thds) end
    for i = 1, thds do
        dbs[i]:wr_stmt("set transaction chunk " .. 10000)
        dbs[i]:wr_stmt("begin")
    end

    local from = 1
    local per_thd = total / thds
    for i = 1, thds do
        to = from + per_thd - 1;
        --print("thd:"..i.." from:"..from.." to:"..to)
        dbs[i]:bind(1, from)
        dbs[i]:bind(2, from + per_thd - 1)
        dbs[i]:async_stmt("insert into t(i) select value from generate_series(?, ?)")
        from = to + 1
    end
    for i = 1, thds do
        dbs[i]:drain()
    end

    for i = 1, thds do
        dbs[i]:async_stmt("commit")
    end
    for i = 1, thds do
        dbs[i]:drain()
    end
end

function verify_err()
    for i = 1, thds do
        dbs[i]:async_stmt("delete from t where i >= 1 limit 5000")
    end
    for i = 1, thds do
        dbs[i]:verify_err()
    end
end

function test()
    thds = 10
    dbs = {}
    connect()
    truncate()
    insert(60000)
    tbl_stats()
    verify_err()
    tbl_stats()
    print("success")
end

test()
