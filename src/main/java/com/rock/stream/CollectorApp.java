package com.rock.stream;

import com.rock.stream.config.ConfigReader;
import com.rock.stream.model.RowData;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

@Slf4j
public class CollectorApp {

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool
                .fromPropertiesFile(CollectorApp.class.getClassLoader().getResourceAsStream("application.properties"))
                .mergeWith(ParameterTool.fromArgs(args));
        ConfigReader configReader = new ConfigReader(parameterTool.getProperties());
        System.setProperty("HADOOP_USER_NAME",configReader.getFlinkHadoopUsername());

        final StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.getConfig().setGlobalJobParameters(parameterTool);
        executionEnvironment.getConfig().setExecutionMode(ExecutionMode.PIPELINED);
        executionEnvironment.getConfig().enableObjectReuse();
        executionEnvironment.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        executionEnvironment.getCheckpointConfig().setCheckpointInterval(configReader.getFlinkCheckpointInterval());
        executionEnvironment.getCheckpointConfig().setCheckpointStorage(configReader.getFlinkCheckpointStorePath());
        executionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(configReader.getFlinkCheckpointMinPauseBetweenCheckpoint());
        executionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        executionEnvironment.getCheckpointConfig().enableUnalignedCheckpoints();
        executionEnvironment.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        executionEnvironment.setParallelism(configReader.getFlinkDefaultParallelism());


        Properties p = new Properties();
        p.setProperty("transaction.timeout.ms", String.valueOf(1000 * 60 * 50));
        p.setProperty("max.request.size","104857600");

        final KafkaSink<RowData> kafkaSink = KafkaSink.<RowData>builder()
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setBootstrapServers(configReader.getKafkaBootstrapServers())
                .setKafkaProducerConfig(p)
                .setTransactionalIdPrefix("cdc")
                .setRecordSerializer(new RoutingKafkaSerializationSchema(configReader.getKafkaProducerPrefix()))
                .build();

        SyncInstanceFactory.create(configReader, executionEnvironment)
                .process()
                .keyBy(RowData::keyStrValues)
                .sinkTo(kafkaSink);

        executionEnvironment.execute(parameterTool.get("flink.job.name"));

    }
}
