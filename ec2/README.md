# Running Flink Jobs on AWS EC2

## Update SSH config

_WARNING_: This doc is specific to our cluster setting and our current Flink deploy. This is not meant to be a general guide for distributed Flink setups.

Add to your `~/.ssh/config` the following line:

```
Include ~/.ssh/config_flink
```

Then you must obtain the `.pem` file for the identity installed in EC2 machines.

Once got that, from this folder:

```
$ ./update_flink_ssh_config path/to/identity_file.pem <jobmanager_IP>
```

The `jobmanager_IP` is the public IP of the machine named `jm` on the AWS console.

_NOTE_: The script `./update_flink_ssh_config` must be executed every time that you stop/start the cluster, because the public IP of the Job Manager changes.

Now you can type `ssh jm` to SSH into the Job Manager machine.

## Start/stop cluster

You won't actually need to start the cluster manually, because the scripts to run experiments are instrumented to do it. Anyways, from the home directory:

```
$ source flinkrc
$ start_cluster.sh
$ ...
$ stop_cluster.sh
```

You can check Flink's UI at `jm_public_IP:8080`.

## Run experiment

From the home:

```
$ source flinkrc
```

That sets environment variables for the evaluation scripts to work. Then, change directory for executing the evaluation scripts:

```
$ cd t-spoon/launch_scripts
$ source evaluation_functions.sh
```

Now you can run experiments:

```
# runs an experiment with a single transactional graph and a single
# stateful operator.
$ launch_series_1tg 1

# you can launch predefined suits:
$ launch_suite_series_1tg
$ ...
$ launch_suite_query
$ ...
# explore `evaluation_functions.sh` to have an overview of them.

# or you can launch the entire evaluation:
$ ./launch_evaluation my_first_evaluation
```

The outputs of each experiment will fall into `~/t-spoon/launch_scripts/results`.
If you run a single expriment, for instance `$ launch_series_1tg 1`, you will have a single file:

```
~/t-spoon/launch_scripts/results/series_1tg_1.json
```

_WARNING_: running the same single experiment more than once overwrites the file.
You can override the result directory with:

```
$ mkdir ~/t-spoon/launch_scripts/results/my_new_dir
$ export RESULTS_DIR=~/t-spoon/launch_scripts/results/my_new_dir
```

before running an experiment.

If you run the entire evaluation:

```
$ ./launch_evaluation my_first_evaluation
```

You will find the results divided into folders under `~/t-spoon/launch_scripts/results/my_first_evaluation`.

## Gather Results and Plot

First of all, move to the right directory and install dependencies for plotting.
From the project directory:

```
$ cd launch_scripts
$ ./install.sh
```

This requires `python-virtualenv` to be installed.
It creates a new virtual environment and installs the requirements into it
(if you have any problem, see the script `install.sh` and try to perform its steps manually).

The best practice is to copy the entire results folder.
From your machine:

```
$ cd t-spoon/launch_scripts
$ scp -r jm:~/t-spoon/launch_scripts/results ./results/last_results
```

Then you can parse the results, and plot them:

```
# parse data from experiment output and persists the result to json files
$ python parse_results.py ./results/last_results
# load the parsed results and generate plots
$ python plot_results.py ./results/last_results
```

# Running VoltDB Benchmarks on AWS EC2

_WARNING_: This doc is specific to our cluster setting and our current VoltDB deploy. This is not meant to be a general guide for distributed VoltDB setups.

_NOTE_: For running benchmarks on EC2 you must complete the [setup for SSH](#-update-ssh-config).

Every command below runs in VoltDB's directory:

```
$ cd t-spoon/voltdb
$ source voltrc
```

## Start/stop cluster

```
$ ./deploy_cluster.sh
$ ...
$ ./destroy_cluster.sh
```

You can check VoltDB's UI at `jm_public_IP:8080`.

## Run experiment

While the cluster is up:

```
$ ./voltdb_install.sh
$ ./run_benchmark.sh
```

To configure an experiment, have a look at `KVConfig` in `AsynchBenchmark.java`:

```java
static class KVConfig extends CLIConfig {
    @Option(desc = "Interval for performance feedback, in seconds.")
    long displayinterval = 5;

    @Option(desc = "Warmup duration in seconds.")
    int warmup = 5;

    @Option(desc = "Number of tuples on which we calculate the benchmark")
    int tuples = 200000;

    @Option(desc = "Comma separated list of the form server[:port] to connect to.")
    String servers = "localhost";

    @Option(desc = "Number of keys to preload.")
    int poolsize = 100000;

    @Option(desc = "Whether to preload a specified number of keys and values.")
    boolean preload = true;

    @Option(desc = "Fraction of ops that are gets (vs puts).")
    double getputratio = 0.0;

    @Option(desc = "Size of keys in bytes.")
    int keysize = 32;

    @Option(desc = "Minimum value size in bytes.")
    int minvaluesize = 1024;

    @Option(desc = "Maximum value size in bytes.")
    int maxvaluesize = 1024;

    @Option(desc = "Number of values considered for each value byte.")
    int entropy = 127;

    @Option(desc = "Compress values on the client side.")
    boolean usecompression = false;

    @Option(desc = "Maximum TPS rate for benchmark.")
    int ratelimit = Integer.MAX_VALUE;

    @Option(desc = "Report latency for async benchmark run.")
    boolean latencyreport = true;

    @Option(desc = "Filename to write raw summary statistics to.")
    String statsfile = "";

    @Override
    public void validate() {
        if (tuples <= 0) exitWithMessageAndUsage("tuples must be > 0");
        if (warmup < 0) exitWithMessageAndUsage("warmup must be >= 0");
        if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
        if (poolsize <= 0) exitWithMessageAndUsage("poolsize must be > 0");
        if (getputratio < 0) exitWithMessageAndUsage("getputratio must be >= 0");
        if (getputratio > 1) exitWithMessageAndUsage("getputratio must be <= 1");

        if (keysize <= 0) exitWithMessageAndUsage("keysize must be > 0");
        if (keysize > 250) exitWithMessageAndUsage("keysize must be <= 250");
        if (minvaluesize <= 0) exitWithMessageAndUsage("minvaluesize must be > 0");
        if (maxvaluesize <= 0) exitWithMessageAndUsage("maxvaluesize must be > 0");
        if (entropy <= 0) exitWithMessageAndUsage("entropy must be > 0");
        if (entropy > 127) exitWithMessageAndUsage("entropy must be <= 127");

        if (ratelimit <= 0) exitWithMessageAndUsage("ratelimit must be > 0");
    }
}
```

You can set options via `--<option>`.
E.g., for testing latency, you can set a rate limit:

```
$ ./run_benchmark.sh --ratelimit 5000
```

## Gather results

There is no programmatic way of extracting them.
Copy paste the results obtained from the terminal.
