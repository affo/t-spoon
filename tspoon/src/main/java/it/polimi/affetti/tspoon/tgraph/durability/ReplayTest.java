package it.polimi.affetti.tspoon.tgraph.durability;

import it.polimi.affetti.tspoon.common.TimestampGenerator;
import it.polimi.affetti.tspoon.metrics.Metric;
import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.Updates;
import it.polimi.affetti.tspoon.tgraph.Vote;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.stream.IntStream;

/**
 * Created by affo on 18/05/18.
 */
public class ReplayTest {
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
                    long nextID = tg[index].nextTimestamp();
                    int partition = i % numberOfPartitions;
                    return createEntry(nextID, nextID, partition);
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
        int checkpointInterval = parameters.getInt("checkpointInterval");
        int numberOfWALs = parameters.getInt("numberOfWALs");
        int numberOfLocalServers = parameters.getInt("numberOfLocalServers");
        int numberOfSources = parameters.getInt("numberOfSources");
        int numberOfPartitions = parameters.getInt("numberOfPartitions");
        int numberOfRounds = parameters.getInt("numberOfRounds", 1);
        int numberOfRecords = inputRate * checkpointInterval;

        // start proxy
        String[] ips = new String[numberOfLocalServers];
        Arrays.fill(ips, "localhost");
        ProxyWALServer proxyWALServer = NetUtils.launchWALServer(
                parameters, numberOfSources, ips);

        // create WALs and local servers
        LocalWALServer[] localWALServers = new LocalWALServer[numberOfLocalServers];
        for (int i = 0; i < localWALServers.length; i++) {
            localWALServers[i] = NetUtils.getServer(NetUtils.ServerType.WAL,
                    new LocalWALServer(numberOfWALs / numberOfLocalServers,
                            proxyWALServer.getIP(), proxyWALServer.getPort()));
        }

        FileWAL[] wals = new FileWAL[numberOfWALs];
        for (int i = 0; i < wals.length; i++) {
            wals[i] = new FileWAL("wal-test" + i + ".tmp.log", true);
            wals[i].open();
            int index = i % localWALServers.length;
            localWALServers[index].addWAL(wals[i]);
        }

        // start clients
        WALClient[] atSource = new WALClient[numberOfSources];
        for (int i = 0; i < atSource.length; i++) {
            atSource[i] = WALClient.get(ips);
        }
        WALClient[] atState = new WALClient[numberOfPartitions];
        for (int i = 0; i < atState.length; i++) {
            atState[i] = WALClient.get(ips);
        }

        System.out.println(">>> Init complete, filling WALs");

        // fillWALs
        long start = System.nanoTime();
        fillWALs(numberOfRecords, numberOfSources, numberOfWALs, numberOfPartitions, wals);
        double delta = (System.nanoTime() - start) * Math.pow(10, -6);

        System.out.println(">>> Filled in " + delta + "[ms], now replaying");

        Semaphore barrier = new Semaphore(0);
        // ok, now we should replay from various clients
        for (int i = 0; i < numberOfRounds; i++) {
            // reload
            int w = 0;
            for (FileWAL wal : wals) {
                long timeMS = wal.forceReload();
                reloadAtSink.add((double) timeMS);
                System.out.println(">>> WAL[" + w + "] loaded from disk");
                w++;
            }

            for (int j = 0; j < atSource.length; j++) {
                int finalJ = j;
                WALClient source = atSource[j];
                pool.submit(() -> {
                    try {
                        measure(reloadAtSource, () -> {
                            Iterator<WALEntry> replay = source.replay(finalJ, numberOfSources);
                            int n = 0;
                            while (replay.hasNext()) {
                                replay.next();
                                n++;
                            }
                            synchronized (recordsAtSource) {
                                recordsAtSource.add((double) n);
                            }
                            System.out.println(">>> SOURCE[" + finalJ + "] replayed");
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

            barrier.acquire(atSource.length + atState.length);

            System.out.println(">>> Round number " + (i + 1) + "/" + numberOfRounds + " finished.");
        }

        // close everything
        for (WALClient client : atState) {
            client.close();
        }

        for (WALClient client : atSource) {
            client.close();
        }

        for (FileWAL wal : wals) {
            wal.close();
        }

        for (LocalWALServer localWALServer : localWALServers) {
            localWALServer.close();
        }

        proxyWALServer.close();
        pool.shutdown();

        // print results
        Thread.sleep(1000);
        System.out.println(">>> Total number of entries (120k is 11MB): " + numberOfRecords);
        System.out.println(">>> Reload at sink[ms]:");
        System.out.println(reloadAtSink);
        System.out.println(">>> Replay at source[ms]:");
        System.out.println(reloadAtSource);
        System.out.println(">>>>>> Number of replays at source:");
        System.out.println(recordsAtSource);
        System.out.println(">>> Replay at state[ms]:");
        System.out.println(reloadAtState);
        System.out.println(">>>>>> Number of replays at state:");
        System.out.println(recordsAtState);
    }

    @FunctionalInterface
    interface WithException {
        void run() throws Exception;
    }
}
