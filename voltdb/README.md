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

Send to every slave node in the cluster:

 - voltdb-home
 - voltrc
 - deploy_conf.template.xml
 - voltdb_deploy.sh

In this README, I assume that every slave gathers files in a folder named
`voltdb` and it can reach the leader node. The leader's IP is `leader` and is
available to every node. For example, in `/etc/hosts`:

```
# /etc/hosts content:

...
192.168.0.1 leader
...
```

On the leader, add to `hosts.csv` the IPs in the cluster (except `leader`) as
comma-separated values.

In a separate shell (on the leader):

```
$ ./voltdb_deploy.sh <no_hosts> leader <sitesperhost>
```

Adapt the file `remote_deploy.sh` (if you need to). It is the one that will be
executed on remote nodes. The command `./voltdb_deploy.sh ... -B` must match the
one above.

Eventually, you can run the cluster:

```
$ ./deploy_cluster.sh
```

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

## NOTE

If you want to run benchmarks by issuing queries to every host in parallel, you
can pass in the option `--servers ...` by using the file `hosts.csv`:

```
$ ./run_benchmark.sh ... --servers localhost,$(cat hosts.csv)
```

or

```
$ ./voter_run.sh ... --servers localhost,$(cat hosts.csv)
```
