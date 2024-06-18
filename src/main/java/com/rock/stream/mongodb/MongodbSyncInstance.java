package com.rock.stream.mongodb;

import com.rock.stream.config.ConfigReader;
import com.rock.stream.model.RowData;
import com.rock.stream.BaseSyncInstance;
import com.rock.stream.mongodb.changestream.RowDataDebeziumDeserializationSchema;
import com.rock.stream.mongodb.oplog.OplogMongoDBSource;
import com.rock.stream.mongodb.oplog.RowDataOplogDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mongodb.source.MongoDBSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MongodbSyncInstance extends BaseSyncInstance {

    private final ConfigReader configReader;

    public MongodbSyncInstance(ConfigReader configReader, StreamExecutionEnvironment executionEnvironment) {
        super(executionEnvironment);
        this.configReader = configReader;
    }

    @Override
    public SingleOutputStreamOperator<RowData> process() throws Exception {

        final MongodbInstanceConfig mongodbInstanceConfig = new MongodbInstanceConfigFactory().getInstanceConfig(configReader);

        if (mongodbInstanceConfig.getType().equals("oplog")) {
            return executionEnvironment.addSource(createOplogSourceFunction(mongodbInstanceConfig)).name(sourceName(mongodbInstanceConfig.getName()));
        } else if (mongodbInstanceConfig.getType().equals("changeStream")) {
            return executionEnvironment.fromSource(createChangeStreamSourceFunction(
                    mongodbInstanceConfig), WatermarkStrategy.noWatermarks(), sourceName(mongodbInstanceConfig.getName()));
        }

        throw new RuntimeException("not support mongodb instance type [" + mongodbInstanceConfig.getType() + "]");
    }

    private MongoDBSource<RowData> createChangeStreamSourceFunction(MongodbInstanceConfig mongodbInstanceConfig) {

        return MongoDBSource.<RowData>builder()
                .hosts(mongodbInstanceConfig.getHosts())
                .username(mongodbInstanceConfig.getUsername())
                .password(mongodbInstanceConfig.getPassword())
                .databaseList(mongodbInstanceConfig.getDatabaseList())
                .collectionList(mongodbInstanceConfig.getCollectionList())
                .pollMaxBatchSize(1024)
                .pollAwaitTimeMillis(1000)
                .startupOptions(mongodbInstanceConfig.getStartupOptions())
                //读取存量数据
                .deserializer(new RowDataDebeziumDeserializationSchema(mongodbInstanceConfig.getTimezone()))
                .build();
    }

    private DebeziumSourceFunction<RowData> createOplogSourceFunction(MongodbInstanceConfig mongodbInstanceConfig) {
        return OplogMongoDBSource.<RowData>builder()
                .hosts(mongodbInstanceConfig.getHosts())
                .username(mongodbInstanceConfig.getUsername())
                .password(mongodbInstanceConfig.getPassword())
                .databaseList(mongodbInstanceConfig.getDatabaseList())
                .collectionList(mongodbInstanceConfig.getCollectionList())
                .pollMaxBatchSize(1024)
                .pollAwaitTimeMillis(1000)
                .deserializer(new RowDataOplogDebeziumDeserializationSchema(mongodbInstanceConfig.getTimezone()))
                .build();
    }


    @Override
    public String name() {
        return null;
    }

    @Override
    public void init() {

    }
}
