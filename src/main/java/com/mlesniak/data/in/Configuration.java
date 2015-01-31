package com.mlesniak.data.in;

import com.mlesniak.runner.RunnerConfiguration;

public class Configuration extends RunnerConfiguration {
    private String server;
    private String path;
    private String filename;
    private String count;
    private String iterations;

    private String partition;
    private String year;
    private String month;
    private String day;

    public String getPartition() {
        return partition;
    }

    public String getYear() {
        return year;
    }

    public String getMonth() {
        return month;
    }

    public String getDay() {
        return day;
    }

    public String getIterations() {
        return iterations;
    }

    public String getFilename() {
        return filename;
    }

    public String getCount() {
        return count;
    }

    public String getPath() {
        return path;
    }

    public String getServer() {
        return server;
    }

    public static Configuration get() {
        return (Configuration) RunnerConfiguration.get();
    }
}
