package it.polimi.affetti.tspoon.tgraph.durability;

import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.Updates;
import it.polimi.affetti.tspoon.tgraph.Vote;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by affo on 13/04/18.
 */
public class WALServiceTest {
    private ProxyWALServer server;
    private final String[] ips = {"localhost", "localhost"};
    private final int numberOfClose = ips.length * 2;
    private LocalWALServer[] localWALServers;
    private FileWAL[] wals;
    private WALClient client;

    @Before
    public void before() throws IOException, InterruptedException {
        server = NetUtils.launchWALServer(ParameterTool.fromMap(new HashMap<>()), 2, ips);

        localWALServers = new LocalWALServer[ips.length];

        for (int i = 0; i < localWALServers.length; i++) {
            localWALServers[i] = NetUtils.getServer(NetUtils.ServerType.WAL,
                    new LocalWALServer(2, server.getIP(), server.getPort()));
        }

        // every local server manages 2 FileWALs
        wals = new FileWAL[numberOfClose];
        for (int i = 0; i < wals.length; i++) {
            wals[i] = localWALServers[i % localWALServers.length].addAndCreateWAL(0, true);
        }

        client = WALClient.get(ips);
        server.waitForJoinCompletion();
    }

    @After
    public void after() throws Exception {
        client.close();
        server.close();

        for (LocalWALServer srv : localWALServers) {
            srv.close();
        }

        for (FileWAL wal : wals) {
            wal.close();
        }
    }

    private void forceReload() throws IOException {
        for (FileWAL wal : wals) {
            wal.waitForCompactionToFinish();
            wal.forceReload();
        }
    }

    @Test
    public void addOneEntryTest() throws IOException, InterruptedException {
        String namespace = "empty_namespace";
        Updates updates = new Updates();
        updates.addUpdate(namespace, "k0", 0.42);
        updates.addUpdate(namespace, "k1", 0.43);
        updates.addUpdate(namespace, "k2", 0.44);

        WALEntry entry = new WALEntry(Vote.COMMIT, -1, -1, updates);
        wals[0].addEntry(entry);

        forceReload();
        Iterator<WALEntry> entryIterator = localWALServers[0].getWrappedWALs()[0].replay(namespace);

        WALEntry nextAndLast = entryIterator.next();

        Assert.assertEquals(entry, nextAndLast);
        Assert.assertFalse(entryIterator.hasNext()); // is the last
    }

    @Test
    public void testEmptyReplay() throws IOException {
        testReplay(client, Collections.emptyList());
    }

    @Test
    public void replayTest() throws IOException {
        List<WALEntry> entries = fillWALs(0, 10000);
        testReplay(client, entries);
    }

    @Test
    public void multipleClientsTest() throws Exception {
        WALClient client2 = WALClient.get(ips);
        List<WALEntry> entries = fillWALs(0, 10000);

        final Exception[] exs = {null, null};

        Thread one = new Thread(() -> {
            try {
                testReplay(client, entries);
            } catch (Exception e) {
                exs[0] = e;
            }
        });

        Thread two = new Thread(() -> {
            try {
                testReplay(client2, entries);
            } catch (Exception e) {
                exs[1] = e;
            }
        });

        one.start();
        two.start();

        one.join();
        two.join();

        for (Exception exception : exs) {
            if (exception != null) {
                throw exception;
            }
        }
    }

    private List<WALEntry> fillWALs(int startInclusive, int endExclusive) throws IOException {
        List<WALEntry> entries = IntStream.range(startInclusive, endExclusive)
                .mapToObj(this::standardEntry)
                .collect(Collectors.toList());

        fillWALs(entries);

        return entries;
    }

    private void fillWALs(List<WALEntry> entries) throws IOException {
        // an entry to everybody
        int walIndex = 0;
        for (WALEntry entry : entries) {
            FileWAL currenWAL = wals[walIndex];

            currenWAL.addEntry(entry);
            walIndex++;
            if (walIndex == wals.length) {
                walIndex = 0;
            }
        }
    }

    private WALEntry standardEntry(int i) {
        Updates updates = new Updates();
        updates.addUpdate("namespace", "k", 42);
        return new WALEntry(Vote.COMMIT, i, i, updates);
    }

    private void testReplay(WALClient client, List<WALEntry> expected) throws IOException {
        forceReload();
        Iterator<WALEntry> actualIterator = client.replay("*");

        List<WALEntry> actualEntries = new ArrayList<>();
        while (actualIterator.hasNext()) {
            actualEntries.add(actualIterator.next());
        }
        actualEntries.sort(Comparator.comparingLong(e -> e.timestamp));

        Assert.assertEquals(expected, actualEntries);
    }

    @Test
    public void snapshotTest() throws IOException, InterruptedException {
        SnapshotClient openOp1 = new SnapshotClient("localhost", NetUtils.GLOBAL_WAL_SERVER_PORT);
        SnapshotClient openOp2 = new SnapshotClient("localhost", NetUtils.GLOBAL_WAL_SERVER_PORT);
        openOp1.open();
        openOp2.open();
        WALClient stateOp = client;

        // first 15 updates
        List<WALEntry> expected = fillWALs(0, 15);

        // now snapshot
        int snapshotWM = 11;
        Thread start1 = new Thread(() -> {
            try {
                openOp1.startSnapshot(snapshotWM);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        start1.start();
        openOp2.startSnapshot(snapshotWM - 3); // the max will win

        start1.join();

        // 5 records more
        expected.addAll(fillWALs(15, 20));

        // some sink commit
        for (int i = 0; i < localWALServers.length / 2; i++) {
            localWALServers[i].commitSnapshot();
        }

        // 5 records more
        expected.addAll(fillWALs(20, 25));

        // lastCommits, ok the snapshot is complete!
        for (int i = localWALServers.length / 2; i < localWALServers.length; i++) {
            localWALServers[i].commitSnapshot();
        }

        forceReload();
        Iterator<WALEntry> replay = stateOp.replay("*");
        List<WALEntry> actual = new ArrayList<>();
        while (replay.hasNext()) {
            actual.add(replay.next());
        }
        actual.sort(Comparator.comparing(e -> e.timestamp));

        long ts;
        do {
            WALEntry first = expected.remove(0);
            ts = first.timestamp;
        } while (ts < snapshotWM);

        Assert.assertEquals(expected, actual);
    }

    /**
     * Tests snapshot while concurrently writing
     */
    @Test
    public void snapshotIntensiveTest() throws IOException, InterruptedException {
        SnapshotClient openOp1 = new SnapshotClient("localhost", NetUtils.GLOBAL_WAL_SERVER_PORT);
        SnapshotClient openOp2 = new SnapshotClient("localhost", NetUtils.GLOBAL_WAL_SERVER_PORT);
        openOp1.open();
        openOp2.open();

        long snapshotWM = 4242;
        int numberOfRecords = 10000;

        // concurrently update wals and snapshot
        fillWALs(0, (int) (snapshotWM + 1));
        List<WALEntry> expected = IntStream.range((int) (snapshotWM + 1), numberOfRecords)
                .mapToObj(this::standardEntry)
                .collect(Collectors.toList());

        Thread filler = new Thread(() -> {
            try {
                fillWALs(expected);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        Thread snapshotter = new Thread(() -> {
            try {
                new Thread(() -> {
                    try {
                        openOp1.startSnapshot(snapshotWM);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }).start();
                openOp2.startSnapshot(snapshotWM);

                for (LocalWALServer localWALServer : localWALServers) {
                    localWALServer.commitSnapshot();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        filler.start();
        snapshotter.start();
        filler.join();
        snapshotter.join();

        for (FileWAL wal : wals) {
            wal.close();
        }

        // manually loadWALs in FileWALs
        List<WALEntry> actual = new ArrayList<>();
        for (FileWAL wal : wals) {
            wal.waitForCompactionToFinish();
            actual.addAll(wal.loadWAL());
        }
        actual.sort(Comparator.comparing(e -> e.timestamp));

        Assert.assertEquals(expected, actual);
    }
}
