package it.polimi.affetti.tspoon.tgraph.twopc;

import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.Updates;
import it.polimi.affetti.tspoon.tgraph.Vote;
import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by affo on 13/04/18.
 */
public class WALServiceTest {
    private WALServer server;
    private WALClient client;

    @Before
    public void before() throws IOException {
        Map<String, String> args = new HashMap<>();
        args.put("par", "4");
        args.put("sourcePar", "2");
        ParameterTool params = ParameterTool.fromMap(args);
        server = NetUtils.launchWALServer(params);
        client = WALClient.get(params);
    }

    @After
    public void after() throws Exception {
        client.close();
        server.close();
    }

    @Test
    public void addOneEntryTest() throws IOException, InterruptedException {
        String namespace = "empty_namespace";
        Updates updates = new Updates();
        updates.addUpdate(namespace, "k0", 0.42);
        updates.addUpdate(namespace, "k1", 0.43);
        updates.addUpdate(namespace, "k2", 0.44);

        WAL.Entry entry = new WAL.Entry(Vote.COMMIT, -1, -1, updates);
        client.addEntry(entry);
        Iterator<WAL.Entry> entryIterator = server.getWrappedWAL().replay(namespace);

        WAL.Entry nextAndLast = entryIterator.next();

        Assert.assertEquals(entry, nextAndLast);
        Assert.assertFalse(entryIterator.hasNext()); // is the last
    }

    @Test
    public void replayTest() throws IOException {
        testReplay(client, 0, 1000);
    }

    @Test
    public void multipleClientsTest() throws Exception {
        WALClient client2 = new WALClient(client.address, client.port);
        client2.init();

        final Exception[] exs = {null, null};

        Thread one = new Thread(() -> {
            try {
                testReplay(client, 0, 1000);
            } catch (Exception e) {
                exs[0] = e;
            }
        });

        Thread two = new Thread(() -> {
            try {
                testReplay(client2, 1000, 3000);
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

    private void testReplay(WALClient client, int startInclusive, int endExclusive) throws IOException {
        Updates updates = new Updates();
        String namespace = String.valueOf(client.hashCode());
        updates.addUpdate(namespace, "k", 42);

        List<WAL.Entry> entries = IntStream.range(startInclusive, endExclusive)
                .mapToObj(i -> new WAL.Entry(Vote.COMMIT, i, i, updates))
                .collect(Collectors.toList());

        for (WAL.Entry entry : entries) {
            client.addEntry(entry);
        }

        Iterator<WAL.Entry> expectedIterator = entries.iterator();
        Iterator<WAL.Entry> actualIterator = client.replay(namespace);

        while (expectedIterator.hasNext()) {
            WAL.Entry expected = expectedIterator.next();
            WAL.Entry actual;
            do {
                actual = actualIterator.next();
            } while (!actual.equals(expected));
        }

        while (actualIterator.hasNext()) {
            WAL.Entry actual = actualIterator.next();
            Assert.assertTrue((actual.timestamp >= endExclusive || actual.timestamp < startInclusive));
        }
    }

    @Test
    public void snapshotTest() throws IOException, InterruptedException {
        WALClient openOp1 = client;
        WALClient openOp2 = new WALClient(client.address, client.port);
        openOp2.init();
        WALClient stateOp = new WALClient(client.address, client.port);
        stateOp.init();
        WALClient closeSink1 = new WALClient(client.address, client.port);
        closeSink1.init();
        WALClient closeSink2 = new WALClient(client.address, client.port);
        closeSink2.init();

        String namespace = "ns";
        Updates sampleUpdate = new Updates();
        sampleUpdate.addUpdate(namespace, "k", 42);

        // first 15 updates
        for (int i = 0; i < 10; i++) {
            closeSink1.addEntry(new WAL.Entry(Vote.COMMIT, i, i, sampleUpdate));
        }
        for (int i = 10; i < 15; i++) {
            closeSink2.addEntry(new WAL.Entry(Vote.COMMIT, i, i, sampleUpdate));
        }

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
        for (int i = 15; i < 20; i++) {
            closeSink2.addEntry(new WAL.Entry(Vote.COMMIT, i, i, sampleUpdate));
        }

        // sink1 commits
        closeSink1.commitSnapshot();

        // 5 records more
        for (int i = 20; i < 25; i++) {
            closeSink1.addEntry(new WAL.Entry(Vote.COMMIT, i, i, sampleUpdate));
        }

        // sink2 commits, ok the snapshot is complete!
        closeSink2.commitSnapshot();

        Iterator<WAL.Entry> replay = stateOp.replay(namespace);
        int i = snapshotWM + 1;
        while (replay.hasNext()) {
            Assert.assertEquals(i, replay.next().timestamp);
            i++;
        }

        Assert.assertEquals(i, 25);
    }
}
