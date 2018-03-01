Download the open-source version of VoltDB
[here](https://downloads.voltdb.com/technologies/server/voltdb-latest.tar.gz)
and extract it to `voltdb-home`.

Deploy VoltDB (pass in proper arguments for clustering):

```
$ ./voltdb_deploy.sh 1 localhost 4
```

Compile benchmark and stored procedure for it. Load schema and the procedure in
VoltDB:

```
$ ./voltdb_install.sh
```

Run the benchmark (you can pass in arguments):

```
$ ./run_benchmark.sh
```
