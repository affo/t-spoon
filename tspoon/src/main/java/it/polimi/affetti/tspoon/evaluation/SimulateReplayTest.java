package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.common.TimestampGenerator;
import it.polimi.affetti.tspoon.metrics.Metric;
import it.polimi.affetti.tspoon.metrics.Report;
import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.Updates;
import it.polimi.affetti.tspoon.tgraph.Vote;
import it.polimi.affetti.tspoon.tgraph.durability.*;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.stream.IntStream;

/**
 * Created by affo on 18/05/18.
 *
 * This experiment emulates what happens upon replay.
 */
public class SimulateReplayTest {
    private static Metric reloadAtSink = new Metric();
    private static Metric reloadAtSource = new Metric();
    private static Metric reloadAtState = new Metric();
    private static Metric recordsAtSource = new Metric();
    private static Metric recordsAtState = new Metric();

    private static ExecutorService pool = Executors.newCachedThreadPool();

    private static String getShardID(int partition) {
        return "balances-" + partition;
    }

    private static WALEntry createEntry(long tid, long ts, int partition) {
        Updates updates = new Updates();
        // 2 updates to simulate transactions
        updates.addUpdate(getShardID(partition), "k1", 42.0);
        updates.addUpdate(getShardID(partition), "k2", 43.0);
        return new WALEntry(Vote.COMMIT, tid, ts, updates);
    }

    private static void fillWALs(
            int numRecords, int numberOfSources, int numberOfWALs, int numberOfPartitions,
            FileWAL[] wals) throws IOException {
        TimestampGenerator tg[] = new TimestampGenerator[numberOfSources];
        for (int i = 0; i < tg.length; i++) {
            tg[i] = new TimestampGenerator(i, numberOfSources, i);
        }

        IntStream.range(0, numRecords)
                .mapToObj(i -> {
                    int index = i % numberOfSources;
                    long nextTS = tg[index].nextTimestamp();
                    long tid = tg[index].toLogical(nextTS);
                    int partition = i % numberOfPartitions;
                    return createEntry(tid, nextTS, partition);
                })
                .forEach(entry -> {
                    int index = (int) (entry.tid % numberOfWALs);
                    wals[index].addEntry(entry);
                });
    }

    private static void measure(Metric metric, WithException cb) throws Exception {
        long start = System.nanoTime();
        cb.run();
        double delta = (System.nanoTime() - start) * Math.pow(10, -6);
        synchronized (metric) {
            metric.add(delta);
        }
    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        int inputRate = parameters.getInt("inputRate");
        int checkpointInterval = parameters.getInt("checkpointInterval", 60);
        int numberOfSources = parameters.getInt("numberOfSources", 1);
        int parallelism = parameters.getInt("parallelism") - numberOfSources;
        String tms = parameters.get("taskmanagers", "localhost");
        String jobmanagerIP = parameters.get("jmIP", "jobmanager");
        int tmID = parameters.getInt("taskManagerID");
        int numberOfRounds = parameters.getInt("rounds", 1);


        String[] taskManagers = tms.split(",");
        int numberOfTaskmanagers = taskManagers.length;
        int totalNumberOfRecords = inputRate * checkpointInterval;
        int numberOfRecords = totalNumberOfRecords / numberOfTaskmanagers;
        if (tmID == 0) {
            numberOfRecords += totalNumberOfRecords % numberOfTaskmanagers;
        }
        int numberOfTasksHosted = parallelism / numberOfTaskmanagers;
        if (tmID == numberOfSources) {
            // Get remaining tasks only if you don't host a source.
            // It can happen only when the number of tasks
            // is not divisible by taskManagers (thus has to be > 1)
            numberOfTasksHosted += parallelism % numberOfTaskmanagers;
        }
        boolean jm = tmID == 0;
        boolean hostingSource = tmID < numberOfSources;


        ProxyWALServer proxyWALServer = null;
        if (jm) {
            System.out.println(">>> Starting proxyWALServer at JobManager");
            // start proxy
            proxyWALServer = NetUtils.launchWALServer(
                    parameters, numberOfSources, taskManagers);
        }

        // create WALs and the local server
        System.out.println(">>> Starting localWALServer at TM" + tmID);
        LocalWALServer localWALServer = NetUtils.getServer(NetUtils.ServerType.WAL,
                new LocalWALServer(jobmanagerIP, NetUtils.GLOBAL_WAL_SERVER_PORT));

        FileWAL[] wals = new FileWAL[numberOfTasksHosted];
        for (int i = 0; i < wals.length; i++) {
            wals[i] = new FileWAL("wal-test" + i + ".tmp.log", true);
            wals[i].open();
            localWALServer.addWAL(wals[i]);
        }

        // start source client only if the tmID allows it
        WALClient atSource = null;
        if (hostingSource) {
            System.out.println(">>> Starting source WALClient at TM" + tmID);
            atSource = WALClient.get(taskManagers);
        }

        WALClient[] atState = new WALClient[numberOfTasksHosted];
        for (int i = 0; i < atState.length; i++) {
            System.out.println(">>> Starting state WALClient at TM" + tmID);
            atState[i] = WALClient.get(taskManagers);
        }

        System.out.println(">>> Init complete, filling WALs");

        // fillWALs
        long start = System.nanoTime();
        fillWALs(numberOfRecords, numberOfSources, numberOfTasksHosted, parallelism, wals);
        double delta = (System.nanoTime() - start) * Math.pow(10, -6);

        System.out.println(">>> Filled in " + delta + "[ms], now replaying");

        // -------------- Reload phase
        for (int i = 0; i < numberOfRounds; i++) {
            // reload
            int w = 0;
            for (FileWAL wal : wals) {
                long timeMS = wal.forceReload();
                reloadAtSink.add((double) timeMS);
                System.out.println(">>> WAL[" + w + "] loaded from disk");
                w++;
            }
        }

        Thread.sleep(5000);
        // Every participant in this test should have reloaded his WAL
        // numberOfRounds times by this time

        // -------------- Replay phase
        Semaphore barrier = new Semaphore(0);
        for (int i = 0; i < numberOfRounds; i++) {
            if (atSource != null) {
                WALClient finalAtSource = atSource;
                pool.submit(() -> {
                    try {
                        measure(reloadAtSource, () -> {
                            Iterator<WALEntry> replay = finalAtSource.replay(tmID, numberOfSources);
                            int n = 0;
                            while (replay.hasNext()) {
                                replay.next();
                                n++;
                            }
                            synchronized (recordsAtSource) {
                                recordsAtSource.add((double) n);
                            }
                            System.out.println(">>> SOURCE[" + tmID + "] replayed");
                        });
                        barrier.release();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }

            for (int j = 0; j < atState.length; j++) {
                int finalJ = j;
                WALClient state = atState[j];
                pool.submit(() -> {
                    try {
                        measure(reloadAtState, () -> {
                            Iterator<WALEntry> replay = state.replay(getShardID(finalJ));
                            int n = 0;
                            while (replay.hasNext()) {
                                replay.next();
                                n++;
                            }
                            synchronized (recordsAtState) {
                                recordsAtState.add((double) n);
                            }
                            System.out.println(">>> STATE[" + finalJ + "] replayed");
                        });
                        barrier.release();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }

            int plus = atSource == null ? 0 : 1;
            barrier.acquire(plus + atState.length);
            System.out.println(">>> Round number " + (i + 1) + "/" + numberOfRounds + " finished.");
        }


        // close everything
        for (WALClient client : atState) {
            client.close();
        }

        if (atSource != null) {
            atSource.close();
        }

        for (FileWAL wal : wals) {
            wal.close();
        }

        localWALServer.close();
        if (proxyWALServer != null) {
            proxyWALServer.close();
        }
        pool.shutdown();

        // print results
        Thread.sleep(1000);
        Report report = new Report("replay-results-tm" + tmID);
        // >>> Total number of entries (120k is 11MB):
        report.addField("number-of-entries", numberOfRecords);
        report.addField("reload-at-sink", reloadAtSink);
        report.addField("replay-at-source", reloadAtSource);
        report.addField("records-at-source", recordsAtSource);
        report.addField("replay-at-state", reloadAtState);
        report.addField("records-at-state", recordsAtState);
        report.writeToFile();
    }

    @FunctionalInterface
    interface WithException {
        void run() throws Exception;
    }
}
