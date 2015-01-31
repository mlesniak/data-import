package com.mlesniak.data.in;

import com.mlesniak.data.in.schema.KeyValue;
import com.mlesniak.runner.BaseRunner;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class Main extends BaseRunner {
    private final Configuration conf;

    public static void main(String[] args) throws Exception {
        BaseRunner.initRunner(Configuration.class, "data-import", args);
        new Main().run();
    }

    public Main() {
        conf = Configuration.get();
    }

    private void run() throws Exception {
        info("Starting import.");

        int iterations = 1;
        if (conf.getIterations() != null) {
            iterations = Integer.parseInt(conf.getIterations());
        }

        for (int i = 0; i < iterations; i++) {
            info("Iteration {} of {}", i, iterations);
            // Is actually an AvroParquetWriter.
            DataWriter<KeyValue> writer = new DataWriter<>(generatePath(), KeyValue.getClassSchema());
            generateObjects(writer);
            writeObjects(writer);
        }

        info("Finish.");
    }

    private void writeObjects(DataWriter<KeyValue> writer) throws IOException {
        debug("Writing objects.");
        writer.close();
    }

    private Path generatePath() {
        Path outputPath = new Path(conf.getServer() + "/" + conf.getPath() + "/");
        outputPath = outputPath.suffix(getPartitionPath() + "/" + conf.getFilename() + "-" + System.currentTimeMillis() + ".pq");
        debug("Output path {}", outputPath);
        return outputPath;
    }

    private String getPartitionPath() {
        if (!StringUtils.equals(conf.getPartition(), "true")) {
            return "";
        }

        int year = Integer.parseInt(conf.getYear());
        int month = Integer.parseInt(conf.getMonth());
        int day = Integer.parseInt(conf.getDay());

        return "/year=" + year + "/month=" + month + "/day=" + day;
    }

    private void generateObjects(DataWriter<KeyValue> writer) throws IOException {
        debug("Generating {} objects.", conf.getCount());
        int count = Integer.parseInt(conf.getCount());

        int year = Integer.parseInt(conf.getYear());
        int month = Integer.parseInt(conf.getMonth());
        int day = Integer.parseInt(conf.getDay());
        info("date {} {} {}", year, month, day);

        for (int i = 0; i < count; i++) {
            KeyValue table = KeyValue.newBuilder()
                    .setYear(year)
                    .setMonth(month)
                    .setDay(day)
                    .setKey(randInt())
                    .setValue(randInt().toString())
                    .build();
            writer.write(table);
        }
    }

    private Integer randInt() {
        return (int) (Math.random() * 1000);
    }
}
