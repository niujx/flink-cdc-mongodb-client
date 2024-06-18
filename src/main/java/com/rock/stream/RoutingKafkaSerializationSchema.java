package com.rock.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rock.stream.model.RowData;
import lombok.SneakyThrows;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

public class RoutingKafkaSerializationSchema implements KafkaRecordSerializationSchema<RowData> {

    private final String prefix;

    public RoutingKafkaSerializationSchema(String prefix) {
        this.prefix = prefix;
    }

    @SneakyThrows
    @Override
    public ProducerRecord<byte[], byte[]> serialize(RowData rowData, KafkaSinkContext context, Long timestamp) {
        String databaseName = rowData.getDatabase();
        String tableName = rowData.getTable();
        String topic = (prefix + "-" + databaseName + "-" + tableName).replace("_", "-");
        final byte[] bytes = new ObjectMapper().writeValueAsBytes(rowData);
        return new ProducerRecord<>(topic, rowData.keyStrValues().getBytes(StandardCharsets.UTF_8), bytes);
    }

}
