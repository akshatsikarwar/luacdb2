#!/home/asikarw1/src/bench/build/cdb2

total_rows = 128
thds = 48
dbs = {}

function connect()
    dbname = "akdb"
    if #arg == 1 then dbname = arg[1] end
    for i = 1, thds do
        table.insert(dbs, comdb2(dbname, "local"))
    end
    db = dbs[1]
end

function truncate()
    db:wr_stmt("create table if not exists t(i integer)")
    db:wr_stmt("truncate table t")
end

function insert(total)
    if total < thds then error("not enough data to insert") end
    if math.fmod(total, thds) ~= 0 then error("need count divisible by " .. thds) end
    for i = 1, thds do
        dbs[i]:wr_stmt("set transaction chunk " .. 10000)
        dbs[i]:wr_stmt("begin")
    end

    from = 1
    per_thd = total / thds
    for i = 1, thds do
        to = from + per_thd - 1;
        print("thd:"..i.." from:"..from.." to:"..to)
        dbs[i]:bind(1, from)
        dbs[i]:bind(2, from + per_thd - 1)
        dbs[i]:async_stmt("insert into t select value from generate_series(?, ?)")
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
        dbs[i]:async_stmt("update t set i = i + 1")
    end
    for i = 1, thds do
        if dbs[i]:verify_err() then num_verify = num_verify + 1 end
    end
end

function scon()
    db:rd_stmt("exec procedure sys.cmd.send('scon')")
    db:drain()
end

function test()
    connect()
    truncate()
    db:wr_stmt("insert into t select value from generate_series(1, " .. tostring(total_rows) ..")")
    scon()

    num_verify = 0
    count = 64 
    comma = ""
    for i = 1, count do
        io.write(comma, i)
        io.flush()
        comma=","
        verify_err()
    end
    io.write("\n")
    print("failed:"..num_verify)
    print("expected min, max:".. tostring(thds * count + 1) ..", ".. tostring(thds * count + total_rows))

    db:rd_stmt("select min(i), max(i), count(i) from t")
    print("actual min, max:".. db:column_value(1) .. "," .. db:column_value(2))
    db:drain()
end

test()
