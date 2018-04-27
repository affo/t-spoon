package it.polimi.affetti.tspoon.tgraph.functions;

import it.polimi.affetti.tspoon.tgraph.BatchID;
import it.polimi.affetti.tspoon.tgraph.Enriched;
import it.polimi.affetti.tspoon.tgraph.TotalOrderEnforcer;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Scheduler<T> extends RichFlatMapFunction<Enriched<T>, Enriched<T>> {
    private Map<String, Enriched<T>> cachedRecords = new HashMap<>();
    private transient TotalOrderEnforcer totalOrderEnforcer;

    private String getMappingKey(long timestamp, BatchID bid) {
        return timestamp + "-" + bid.getDottedRepresentation();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        totalOrderEnforcer = new TotalOrderEnforcer();
    }

    @Override
    public void flatMap(Enriched<T> enriched, Collector<Enriched<T>> collector) throws Exception {
        long timestamp = enriched.metadata.timestamp;
        BatchID batchID = enriched.metadata.batchID;

        cachedRecords.put(getMappingKey(timestamp, batchID), enriched);
        totalOrderEnforcer.addElement(timestamp, batchID);

        for (Map.Entry<Long, List<BatchID>> bids : totalOrderEnforcer.next().entrySet()) {
            long ts = bids.getKey();

            List<BatchID> batch = bids.getValue();
            // flatten the batch id
            long tid = batch.get(0).getTid();
            List<BatchID> flattenedIdSpace = new BatchID(tid).addStep(batch.size());
            Iterator<BatchID> flattenedIdSpaceIterator = flattenedIdSpace.iterator();

            for (BatchID bid : batch) {
                String key = getMappingKey(ts, bid);
                Enriched<T> out = cachedRecords.remove(key);
                // reset the batchID
                out.metadata.batchID = flattenedIdSpaceIterator.next();
                collector.collect(out);
            }
        }
    }
}
