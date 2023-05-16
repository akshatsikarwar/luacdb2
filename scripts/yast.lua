#!/home/asikarw1/src/luacdb2/build/luacdb2

local dbname = "akdb"
local tier = "local"
local tbl = 't'

if #arg >= 1 then dbname = arg[1] end
if #arg >= 2 then tier = arg[2] end
if #arg >= 3 then tbl = arg[3] end

local db = comdb2(dbname, tier)

db:rd_stmt(string.format('select count(*) from comdb2_tables where tablename="%s"', tbl))
if db:column_value(1) ~= 1 then
    --db:wr_stmt(string.format('drop table if exists "%s"', tbl))
    db:drain()
    db:wr_stmt(string.format('create table "%s" {schema{int x int y}}', tbl))
else
    db:drain()
end

db:rd_stmt("select name, value from comdb2_tunables where name in ('dtastripe', 'reorder_socksql_no_deadlock','reorder_idx_writes')")
while db:next_record() do
    print(db:column_value(1), db:column_value(2))
end

function create_test_data(count)
    db:wr_stmt(string.format('delete from "%s" where 1', tbl))
    for x = 1, count do
        for y = 1, count do
            db:wr_stmt(string.format('insert into "%s" values(%d, %d)', tbl, x, y))
        end
    end
end

function create_test_data_cte(count)
    --don't get distribution across stripes in single stmt
    db:wr_stmt(string.format('with cte(val) as (select value from generate_series(1, %d)) \z
                              insert into "%s" select a.val, b.val from cte a join cte b',
                              count, tbl))
end

function print_last()
    print(string.format("last stmt:\n%s\nlast result:", last_stmt))
    for _, row in ipairs(last_result) do
        for i, col in ipairs(row) do
            if i % 2 == 0 then
                io.write('=')
            elseif i > 1 then
                io.write(', ')
            end
            io.write(col)
        end
        print('')
    end
end

function check(stmt, count)
    local stmt = string.format(stmt, y)
    db:rd_stmt(stmt)
    if db:column_value(1) == count then 
        db:drain()
        return
    end
    print("failed:")
    print(stmt)
    print(string.format('expected %d, found %d', count, db:column_value(1)))
    --print_last()
    os.exit()
end

function rd_stmt(stmt)
    last_stmt = stmt
    last_result = {}
    db:rd_stmt(stmt)
    while db:next_record() do
        local row = {}
        for i = 1, db:num_columns() do
            table.insert(row, db:column_name(i))
            table.insert(row, db:column_value(i))
        end
        table.insert(last_result, row)
    end
end

local count = 0
while true do
    count = count + 1
    if count == 100 then break end
    create_test_data(6)
    check(string.format('SELECT count(*) FROM "%s"', tbl), 36)

    db:wr_stmt(string.format('UPDATE "%s" SET y=1 WHERE x=1', tbl))
    check(string.format('SELECT count(*) FROM "%s" WHERE y=1', tbl), 11)

    create_test_data(6)
    db:wr_stmt(string.format('UPDATE "%s" SET y=1 WHERE x=1 LIMIT 5', tbl))
    check(string.format('SELECT count(*) FROM "%s" WHERE y=1', tbl), 10)

    db:wr_stmt(string.format('UPDATE "%s" SET y=2 WHERE x=2 ORDER BY x LIMIT 5', tbl))
    check(string.format('SELECT count(*) FROM "%s" WHERE y=2', tbl), 9)

    create_test_data(6)
    db:wr_stmt(string.format('UPDATE "%s" SET y=2 WHERE x=2 ORDER BY x LIMIT 5 OFFSET 2', tbl))
    check(string.format('SELECT count(*) FROM "%s" WHERE y=1', tbl), 6)

    db:wr_stmt(string.format('UPDATE "%s" SET y=2 WHERE x=2 ORDER BY x LIMIT 5 OFFSET -2', tbl))
    check(string.format('SELECT count(*) FROM "%s" WHERE y=1', tbl), 5)

    db:wr_stmt(string.format('UPDATE "%s" SET y=3 WHERE x=3 ORDER BY x LIMIT 2,  -5', tbl))
    check(string.format('SELECT count(*) FROM "%s" WHERE y=3', tbl), 8)

    db:wr_stmt(string.format('UPDATE "%s" SET y=3 WHERE x=3 ORDER BY x LIMIT -2,  5', tbl))
    check(string.format('SELECT count(*) FROM "%s" WHERE y=3', tbl), 10)

    db:wr_stmt(string.format('UPDATE "%s" SET y=4 WHERE x=4 ORDER BY x LIMIT -2,  -5', tbl))
    check(string.format('SELECT count(*) FROM "%s" WHERE y=4', tbl), 9)

    create_test_data(6)
    db:wr_stmt(string.format('UPDATE "%s" SET y=4 WHERE x=5 ORDER BY x LIMIT 2,  5', tbl))
    check(string.format('SELECT count(*) FROM "%s" WHERE y=4', tbl), 9)

    db:wr_stmt(string.format('UPDATE "%s" SET y=4 WHERE x=6 ORDER BY x LIMIT 5 OFFSET 5', tbl))
    check(string.format('SELECT count(*) FROM "%s" WHERE y=1', tbl), 6)

    db:wr_stmt(string.format('UPDATE "%s" SET y=1 WHERE x=1 ORDER BY x LIMIT 50 OFFSET 30', tbl))
    check(string.format('SELECT count(*) FROM "%s" WHERE y=1', tbl), 6)

    db:wr_stmt(string.format('UPDATE "%s" SET y=1 WHERE x=2 ORDER BY x LIMIT 30,  50', tbl))
    check(string.format('SELECT count(*) FROM "%s" WHERE y=1', tbl), 6)

    db:wr_stmt(string.format('UPDATE "%s" SET y=1 WHERE x=3 ORDER BY x LIMIT 50 OFFSET 50', tbl))
    check(string.format('SELECT count(*) FROM "%s" WHERE y=1', tbl), 6)
end
