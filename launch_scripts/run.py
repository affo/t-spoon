import requests
import sys, os, time, json, math
from requests.exceptions import ConnectionError

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print "Please pass in <output>, <main class>, <jar file path>, and <program arguments>"
        sys.exit(1)

    output_file = sys.argv[1]
    main_class = sys.argv[2]
    jar_path = sys.argv[3]
    args = sys.argv[4]
    # For now, we don't allow to send jobs from outside.
    # The client must be co-located with the JobManager
    base_url = "http://localhost:8081"
    polling_interval = 20
    errors_threshold = 10
    experiment_deadline = 10 * 60 # 10 minutes

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

    def execute_job(arguments):
        run_url = base_url + "/jars/" + fname + "/run?" + \
                "entry-class=" + main_class + "&" + \
                "program-args=" + arguments
        resp = requests.post(run_url)
        resp = json.loads(resp.content)
        if resp.get("error") != None:
            print resp["error"]
            return {}, 1

        job_id = resp["jobid"]
        print job_id

        # start polling until the end of the job...
        number_of_polls = experiment_deadline / polling_interval
        iteration = 0
        errors = 0
        status = "RUNNING"
        url = base_url + "/jobs/" + job_id
        while status == "RUNNING" and iteration < number_of_polls:
            print "{} - Polling job status every {} seconds: {}".format(iteration, polling_interval, status)
            try:
                resp = requests.get(url)
                resp = json.loads(resp.content)
                status = resp["state"]
                errors = 0 # reset errors

                no_finished = int(resp['status-counts']['FINISHED'])
                if no_finished > 4: # some task have finished
                    print 'Some task have finished, stopping job...'
                    break
            except ConnectionError:
                # it could happen for JobManager congestion
                print "{} - Connection error...".format(iteration)
                errors += 1
                if errors > errors_threshold:
                    return {}, 1

            iteration += 1
            time.sleep(polling_interval)

        exit_code = 0
        additional_notes = {}
        if status == "RUNNING":
            # the job is still running after the deadline: kill the job
            print "Canceling the job because deadline exceeded..."
            requests.delete(url + "/cancel")
            print "Job canceled"
            additional_notes["job_canceled"] = "Deadline exceeded: {}s".format(experiment_deadline)
            exit_code = 2

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

        if status != "FINISHED":
            # save the exceptions, if any
            resp = requests.get(url + "/exceptions")
            execution_results["exceptions"] = json.loads(resp.content)
            if len(execution_results["exceptions"]["all-exceptions"]) > 0:
                exit_code = 1

        execution_results["additional-notes"] = additional_notes

        def parse_acc_value(value):
            try:
                return json.loads(value)
            except:
                return value

        accumulators = {acc["name"]: parse_acc_value(acc["value"]) for acc in accumulators}
        execution_results["accumulators"] = accumulators
        return execution_results, exit_code

    results = {}
    exit_code = 0
    try:
        results, exit_code = execute_job(args)
    except Exception as e:
        results["exception"] = str(e)
        exit_code = 1

    print json.dumps(results, indent=4, sort_keys=True)
    with open(output_file, "w") as f:
        json.dump(results, f, indent=4, sort_keys=True)

    sys.exit(exit_code)
