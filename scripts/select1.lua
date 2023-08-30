#!/home/asikarw1/src/luacdb2/build/luacdb2

local function warmup()
    db:rd_stmt("explain query plan select count(*) from listing where id is not null")
    print(db:column_value(4))
    db:drain()
    db:rd_stmt("select count(*) from listing where id is not null")
    db:drain()

    db:rd_stmt("explain query plan select count(*) from listing where msgid is not null")
    print(db:column_value(4))
    db:drain()
    db:rd_stmt("select count(*) from listing where msgid is not null")
    db:drain()
end

local function realwork()
    local id =  math.random(0, 999999)
    db:bind(1, id)
    db:rd_stmt("select id, msgid from listing where id = ?")
    if db:column_value(1) ~= id then error('bad bad') end
    db:drain()
end

local function select1(num)
    db:rd_stmt("select " .. tostring(num))
    if db:column_value(1) ~= num then error('bad bad') end
    db:drain()
end

local function bench(iterations)
    for i = 1, iterations do
        --realwork()
        select1(i)
    end
end

local function main()
    disable_sockpool()
    db = comdb2(argv[1], "local")

    if #argv >= 2 and argv[2] == 'warm' then
        return warmup()
    end

    math.randomseed(os.time())
    local iterations = 1000000

    local start = gettimeofday()
    bench(iterations)
    local fin = gettimeofday()

    local diff = timersub(fin, start)
    print (string.format("iterations:%d", iterations))
    print (string.format("elapsed time: %ds.%dus", diff.sec, diff.usec))
end

main()
