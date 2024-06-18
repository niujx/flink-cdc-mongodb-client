package com.rock.stream.config;

import org.apache.commons.lang3.math.NumberUtils;

import java.util.Properties;

public class BaseConfigReader {

    public static final String FLINK_JOB_NAME = "flink.job.name";

    public static final String FLINK_HADOOP_USERNAME = "flink.hadoop.username";

    public static final String FLINK_CHECKPOINT_INTERVAL = "flink.checkpoint.interval";
    public static final String FLINK_CHECKPOINT_TIMEOUT = "flink.checkpoint.timeout";

    public static final String FLINK_CHECKPOINT_MIN_PAUSE_BETWEEN_CHECKPOINT = "flink.checkpoint.min.pause.between.checkpoint";

    public static final String FLINK_CHECKPOINT_STORE_PATH = "flink.checkpoint.store.path";

    public static final String FLINK_DEFAULT_PARALLELISM = "flink.default.parallelism";


    public final Properties properties;

    public BaseConfigReader(Properties properties) {
        this.properties = properties;
    }

    public String getFlinkHadoopUsername() {
        return properties.getProperty(FLINK_HADOOP_USERNAME);
    }

    public Long getFlinkCheckpointInterval() {
        return NumberUtils.toLong(properties.getProperty(FLINK_CHECKPOINT_INTERVAL), 60000L);
    }

    public Long getFlinkCheckpointTimeout() {
        return NumberUtils.toLong(properties.getProperty(FLINK_CHECKPOINT_TIMEOUT), 1000 * 60 * 5L);
    }

    public Long getFlinkCheckpointMinPauseBetweenCheckpoint() {
        return NumberUtils.toLong(properties.getProperty(FLINK_CHECKPOINT_MIN_PAUSE_BETWEEN_CHECKPOINT), 1000 * 60 * 5L);
    }
    public String getFlinkCheckpointStorePath() {
        return properties.getProperty(FLINK_CHECKPOINT_STORE_PATH);
    }

    public Integer getFlinkDefaultParallelism() {
        return NumberUtils.toInt(properties.getProperty(FLINK_DEFAULT_PARALLELISM), 7);
    }

}
