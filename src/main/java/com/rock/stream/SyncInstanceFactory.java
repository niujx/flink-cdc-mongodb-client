package com.rock.stream;

import com.rock.stream.config.ConfigReader;
import com.rock.stream.mongodb.MongodbSyncInstance;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SyncInstanceFactory {

    public static SyncInstance create(ConfigReader configReader, StreamExecutionEnvironment executionEnvironment) {
        SyncInstance instance = new MongodbSyncInstance(configReader, executionEnvironment);
        instance.init();
        return instance;
    }

}
