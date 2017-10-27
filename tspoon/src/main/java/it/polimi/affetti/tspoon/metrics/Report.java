package it.polimi.affetti.tspoon.metrics;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.flink.api.common.JobExecutionResult;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by affo on 02/08/17.
 */
public class Report {
    private final String fileName;
    private Map<String, Object> fields = new HashMap<>();
    private static List<String> accumulatorNames = new LinkedList<>();

    /**
     * Without extension.
     * .json will be added automatically
     */
    public Report(String fileNameWithoutExtension) {
        this.fileName = fileNameWithoutExtension + ".json";
    }

    public void addField(String key, Object field) {
        fields.put(key, field);
    }

    public void updateField(String outerKey, String key, Object obj) {
        Map field = (Map) fields.get(outerKey);
        field.put(key, obj);
    }

    public void addFields(Map<String, ?> fields) {
        this.fields.putAll(fields);
    }

    public static void registerAccumulator(String accumulatorName) {
        accumulatorNames.add(accumulatorName);
    }

    public void addAccumulators(JobExecutionResult result) {
        for (String accumulatorName : accumulatorNames) {
            addField(accumulatorName, result.getAccumulatorResult(accumulatorName));
        }
    }

    public void writeToFile() throws IOException {
        Gson gson = new GsonBuilder()
                .setPrettyPrinting()
                .serializeSpecialFloatingPointValues()
                .registerTypeAdapter(Metric.class, new MetricSerializer())
                .create();

        String converted = gson.toJson(fields);
        System.out.println(">>> BEGIN report");
        System.out.println(converted);
        System.out.println("<<< END report");

        Writer writer = new FileWriter(fileName, false);
        writer.write(converted);
        writer.flush();
        writer.close();

        System.out.println(">>> Report written to " + fileName);
    }
}
