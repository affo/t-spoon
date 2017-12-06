import requests
import sys, os, time, json, math

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print "Please pass in <output>, <main class>, <jar file path>, and <program arguments>"
        exit(1)

    output_file = sys.argv[1]
    main_class = sys.argv[2]
    jar_path = sys.argv[3]
    args = sys.argv[4]
    # For now, we don't allow to send jobs from outside.
    # The client must be co-located with the JobManager
    base_url = "http://localhost:8081"

    resp = requests.post(
            base_url + "/jars/upload",
            files={
                "jarfile": (
                    os.path.basename(jar_path),
                    open(jar_path, "rb"),
                    "application/x-java-archive"
            )
        }
    )

    if resp.status_code != 200:
        print resp.content
        exit(1)

    resp = json.loads(resp.content)
    fname = resp["filename"]

    time.sleep(2)

    def execute_job(arguments, polling_interval=5):
        run_url = base_url + "/jars/" + fname + "/run?" + \
                "entry-class=" + main_class + "&" + \
                "program-args=" + arguments
        resp = requests.post(run_url)
        resp = json.loads(resp.content)
        if resp.get("error") != None:
            print resp["error"]
            exit(1)

        job_id = resp["jobid"]
        print job_id

        # start polling until the end of the job...
        status = "RUNNING"
        url = base_url + "/jobs/" + job_id
        while status == "RUNNING":
            resp = requests.get(url)
            resp = json.loads(resp.content)
            status = resp["state"]
            print "Polling job status every {} seconds: {}".format(polling_interval, status)
            time.sleep(polling_interval)

        # gathering results now
        execution_results = {}
        resp = requests.get(url + "/config")
        config = json.loads(resp.content)
        execution_results["config"] = config

        resp = requests.get(url + "/plan")
        plan = json.loads(resp.content)
        execution_results["plan"] = plan

        resp = requests.get(url + "/accumulators")
        accumulators = json.loads(resp.content)["user-task-accumulators"]

        def parse_acc_value(value):
            try:
                return json.loads(value)
            except:
                return value

        accumulators = {acc["name"]: parse_acc_value(acc["value"]) for acc in accumulators}
        execution_results["accumulators"] = accumulators
        return execution_results

    results = execute_job(args)
    results["additional-notes"] = {}

    throughput = float(results["accumulators"]["throughput"]["mean"])
    percentage = 0.8
    input_rate = int(math.floor(throughput * percentage))

    latency_n_rec = 20000
    latency_sled = 0
    latency_experiment_message = "Executing latency experiment with input rate {} * {} = {}" \
        .format(throughput, percentage, input_rate)
    results["additional-notes"]["inputRate"] = latency_experiment_message
    results["additional-notes"]["nRec"] = "Using nRec {} and sled {}" \
            .format(latency_n_rec, latency_sled)
    print latency_experiment_message

    time.sleep(5)

    latency_results = execute_job(
            "{} --inputRate {} --nRec {} --sled {}" \
                    .format(args, input_rate, latency_n_rec, latency_sled))
    results["accumulators"]["timestamp-deltas"]["latency"] = \
            latency_results["accumulators"]["timestamp-deltas"]["latency"]
    results["config"]["execution-config"]["user-config"]["inputRate"] = input_rate
    print json.dumps(results, indent=4, sort_keys=True)

    with open(output_file, "w") as f:
        json.dump(results, f)

