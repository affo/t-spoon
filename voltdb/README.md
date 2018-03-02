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

## Cluster deploy

Send to your nodes:

 - voltdb-home
 - voltrc
 - deploy_conf.template.xml
 - voltdb_deploy.sh

In this README, I assume that every file is in a folder named `voltdb`.

Add to `hosts.csv` the IPs in the cluster (except `localhost`) as
comma-separated values.

In a separate shell, on the leader:

```
$ ./voltdb_deploy.sh <no_hosts> localhost <sitesperhost>
```

Adapt the file `remote_deploy.sh` (if you need to), that is the one that will be
executed on remote nodes. The command `./voltdb_deploy.sh ... -B` should match
the one above except for the IP of the leader (not `localhost` anymore).

Eventually, you can run the cluster.

## Running Voter benchmark

If you want to run the voter benchmark, copy the voter script in the voter
folder (I suppose that your `VOLT_HOME` is `voltdb-home`):

```
$ cp voter_run.sh voltdb-home/examples/voter
```

The deploy VoltDB (see above) and run the benchmark:

```
$ cd voltdb-home/examples/voter
$ ./voter_run.sh init
$ ./voter_run.sh client ?<AsyncBenchmark params...>
```
