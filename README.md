* `argv` holds cmd-line parameters
* `getenv` / `setenv`
* `gettimeofday` / `timersub`
* `sleep` / `sleepms`

After `run_statement`, call `next_record` until it returns `false`. To finish a
statement sooner, call `drain` which will read till end. For write-statements
(e.g. `set`, `begin`, `commit`, etc) call `wr_stmt` which does not require to
be drained.

To run concurrent requests, call `async_stmt` which invokes `run_statement` in
a background thread. See example 2.

*WIP*

Errors terminate execution immediately. If error is expected (e.g. when testing
concurrent updates and expecting a verify-error), call `verify_error` instead of
`next_record`.

**Example 1:**

```lua
#!/path/to/luacdb2

print(string.format("number of cmd-line args:%d", #argv))

setenv("COMDB2_CONFIG_USE_ENV_VARS", 0)

start = gettimeofday()
db = comdb2("dbname", "tier")
finish = gettimeofday()
elapsed = timersub(finish, start)
print(string.format("cdb2_open took %ds.%dus", elapsed.sec, elapsed.usec))

db:bind("h", "hello")
db:bind("w", "world")
db:run_statement("select @h union select @w order by 1")
while db:next_record() do
    print(db:column_value(1))
end

db:bind(1, "hello")
db:bind(2, "world")
db:run_statement("select ? union select ? order by 1")
while db:next_record() do
    print(db:column_value(1))
end

db:bind_blob("bval", "600dcafe")
db:run_statement("select @bval")
while db:next_record() do
    print(db:column_value(1))
end
```

**Example 2:**

```lua
#!/path/to/luacdb2
dbs = {}
for i = 1, 10 do
	db = comdb2("dbname", "tier")
	db:async_stmt(string.format("select sleep(%d)", i))
	table.insert(dbs, db)
end
start = gettimeofday()
for i, db in ipairs(dbs) do
	while db:next_record() do
		val = db:column_value(1)
	end
	finish = gettimeofday()
	elapsed = timersub(finish, start)
	print(string.format("request:%d time-taken:%ds.%dus val:%d", i, elapsed.sec, elapsed.usec, val))
end
```
