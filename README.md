# Backup/restore small Cassandra keyspaces

A script that uses `COPY TO`, and `COPY FROM` to make a backup of
Cassandra keyspaces in a compressed text format. According to
[this](https://docs.datastax.com/en/cql-oss/3.x/cql/cql_reference/cqlshCopy.html)
it is recommended to use this tool with tables that have less than
2e+6 rows.

```
> ./cassandra_backup.sh --help
```
provides a list of available options.

All backups are placed in the `--dumps` directory with the format:
```
./<--dumps>/<keyspace>/<keyspace>.cqlsh
./<--dumps>/<keyspace>/<table1>.csv.gz
./<--dumps>/<keyspace>/<table2>.csv.gz
...
```
The `*.cqlsh` is a script to generate the keyspace with tables. The
`*.csv.gz` files contain the table data.

## An example

```
> ./cassandra_backup.sh list_keyspaces
keyspace1 keyspace2 keyspace3 system_schema system system_traces system_auth system_views system_distributed system_virtual_schema
> ./cassandra_backup.sh backup  --host=host1 --keyspaces="keyspace1 keyspace2"
> ./cassandra_backup.sh restore --host=host2
```
creates a backup of `keyspace1` and `keyspace2` in the `./dumps`
directory from the `host1`, and consequently regenerates those
keyspaces on the `host2`.

For slow networks, consider adjusting the following parameters:
```
--request-timeout,--connect-timeout
--chunksize,--numprocesses,--maxattempts
```

If you would like to adjust the replication strategy, consider using
something like this:
```
> cp -rl dumps dumps_new
> find dumps_new -name '*.cqlsh' -print0 | \
    xargs -0 sed sed -i "s|'replication_factor': '2'|'replication_factor': '3'|g"
> ./cassandra_backup.sh restore --host=host2 --dumps=./dumps_new
```

## Backup and prune older backups

The `dobackup.sh` script can be used to perform periodic backups: it
calls `./cassandra_backup.sh` and purges older backups. It is executed
in the git root directory.

Use it on your own risk.
