#!/home/asikarw1/src/luacdb2/build/luacdb2

function args()
    dbname = "akdb"
    tier = "local"
    tbl = "t"
    thds = 10 
    rows = 100000
    if #argv >= 1 and (argv[1] == "--help" or argv[1] == "-h") then
        print("Usage: bench.lua [dbname] [tier] [tbl] [thds] [rows]")
        os.exit(0)
    end
    if #argv >= 1 then dbname = argv[1] end
    if #argv >= 2 then tier = argv[2] end
    if #argv >= 3 then tbl = argv[3] end
    if #argv >= 4 then thds = tonumber(argv[4]) end
    if #argv >= 5 then rows = tonumber(argv[5]) end
    print(string.format("db:%s  tier:%s  tbl:%s  thds:%d  rows/thd:%d  total-rows:%d", dbname, tier, tbl, thds, rows, thds * rows))
end

function connect()
    dbs = {}
    for i = 1, thds do
        table.insert(dbs, comdb2(dbname, tier))
        db = dbs[#dbs]
        db:rd_stmt("SELECT comdb2_dbname(), comdb2_host(), comdb2_semver()")
        print(string.format("thd:%2d   dbname:%s/%s   host:%s   version:%s", i, db:column_value(1), tier, db:column_value(2), db:column_value(3)))
        db:drain()
    end
end


function tbl_stats()
    db:wr_stmt("PUT TUNABLE parallel_count 1")
    db:rd_stmt(string.format("SELECT count(*) FROM %s", tbl))
    print("total rows:" .. db:column_value(1))
    db:drain()
    db:rd_stmt(string.format("SELECT min(i), max(i) FROM %s", tbl))
    local min = db:column_value(1)
    local max = db:column_value(2)
    if min == nil then min = 'nil' end
    if max == nil then max = 'nil' end
    print("min:" .. min .. " max:" .. max)
    db:drain()
end

function insert()
    for i = 1, thds do
        dbs[i]:wr_stmt("SET TRANSACTION CHUNK 10000")
        dbs[i]:wr_stmt("BEGIN")
        ins = string.format("INSERT INTO %s SELECT value, randomblob(1024), 'hello, world!' FROM generate_series(1, %d)", tbl, rows)
        if i == 1 then
            print(ins .. ' X ' .. thds)
        end
        dbs[i]:async_stmt(ins)
    end
    for i = 1, thds do
        dbs[i]:drain()
        dbs[i]:wr_stmt("COMMIT")
    end
end

function process_dels(dels)
    local failed = {}
    for _, del in ipairs(dels) do
        print(del .. ' X ' .. thds)
        for i = 1, thds do
            dbs[i]:async_stmt(del)
        end
        local fail = 0
        for i = 1, thds do
            local rc = dbs[i]:verify_err()
            if not rc then
                fail = fail + 1
            end
            if rc == nil then -- something other than verify_err
                print(dbs[i]:last_err())
            end
        end
        if fail > 0 then
            table.insert(failed, del)
        end
    end
    return failed
end

function verify_err()
    local dels = {}
    for i = 1, rows / 5000  do
        table.insert(dels, string.format("DELETE FROM %s WHERE i >= %d LIMIT 5000", tbl, (i - 1) * 5000))
    end
    while #dels > 0 do -- keep running until nothing fails
        dels = process_dels(dels)
    end
end

function time_it(func, ...)
    local start = os.time()
    func(...)
    local stop = os.time()
    print("time: " .. os.difftime(stop, start))
end

function tunable_stats()
    db:rd_stmt("SELECT name, value FROM comdb2_tunables WHERE name IN ('dtastripe', 'reorder_socksql_no_deadlock','reorder_idx_writes')")
    while db:next_record() do
        print(db:column_value(1), db:column_value(2))
    end
end

function test()
    args()
    connect()
    tunable_stats()

    db:wr_stmt(string.format("DROP TABLE IF EXISTS %s", tbl))
    db:wr_stmt(string.format("CREATE TABLE IF NOT EXISTS %s(i INTEGER INDEX, b BLOB, s CSTRING(64))", tbl))

    time_it(insert)
    tbl_stats()
    time_it(verify_err)
    tbl_stats()
    print("success")
end

test()
