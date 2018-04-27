package it.polimi.affetti.tspoon.evaluation;

import it.polimi.affetti.tspoon.runtime.NetUtils;
import it.polimi.affetti.tspoon.tgraph.twopc.WAL;
import it.polimi.affetti.tspoon.tgraph.twopc.WALClient;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by affo on 29/07/17.
 *
 * Run this to get the entries replayed and measure replay time
 */
public class SimulateRecovery {
    public static void main(String[] args) throws Exception {
        WALClient wal = new WALClient("localhost", NetUtils.WAL_SERVER_PORT);
        wal.init();

        List<WAL.Entry> entries = new ArrayList<>();

        long start = System.nanoTime();
        Iterator<WAL.Entry> replayed = wal.replay("balances-0");
        while (replayed.hasNext()) {
            WAL.Entry next = replayed.next();
            entries.add(next);
        }
        double delta = (System.nanoTime() - start) / Math.pow(10, 6);

        for (WAL.Entry entry : entries) {
            Map<String, Object> updates = entry.updates.getUpdatesFor("balances", 0);
            System.out.println(updates);
        }

        long wm = wal.getSnapshotInProgressWatermark();

        System.out.println();
        System.out.println(">>> WM:\t\t" + wm);
        System.out.println(">>> n:\t\t" + entries.size());
        System.out.println(">>> ET(ms):\t\t" + delta);
    }
}
