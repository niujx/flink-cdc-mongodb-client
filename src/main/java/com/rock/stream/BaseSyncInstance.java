package com.rock.stream;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class BaseSyncInstance implements SyncInstance{

    protected ParameterTool parameterTool;
    protected StreamExecutionEnvironment executionEnvironment;

    protected BaseSyncInstance(StreamExecutionEnvironment executionEnvironment) {
        this.executionEnvironment = executionEnvironment;
    }

    protected ParameterTool getParameterTool() {
        return parameterTool;
    }

    protected String sourceName(String name) {
        return String.format("source-%s", name);
    }
}
