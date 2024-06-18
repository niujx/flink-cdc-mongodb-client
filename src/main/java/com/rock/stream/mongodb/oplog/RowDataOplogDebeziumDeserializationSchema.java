package com.rock.stream.mongodb.oplog;

import com.google.common.collect.Lists;
import com.rock.stream.model.RowData;
import com.rock.stream.mongodb.changestream.RowDataDebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.List;
import java.util.Map;

public class RowDataOplogDebeziumDeserializationSchema extends RowDataDebeziumDeserializationSchema {

    private static final String OPERATION = "op";

    private static final String ID_FIELD = "_id";

    private static final String AFTER = "after";

    private static final String SOURCE = "source";

    private static final String DB = "db";

    private static final String PATCH = "patch";

    private static final String TIMESTAMP = "ts_ms";

    private static final String COLLECTION = "collection";

    private static final String SET = "$set";

    private static final String UNSET = "$unset";


    public RowDataOplogDebeziumDeserializationSchema(String timeZone) {
        super(timeZone);
    }

    @Override
    public void deserialize(SourceRecord record, Collector<RowData> out) throws Exception {
        final Struct value = (Struct) record.value();
        final Struct key = (Struct) record.key();
        Envelope.Operation op = extractOperation(value);
        String database = extractDatabase(value);
        String tableName = extractTableName(value);
        long ts = extractTimestamp(value);

        Map<String, Object> columnData = extractColumnData(op, value);
        List<RowData.Field> fields = extractFields(value);
        final Object id = StringUtils.remove(key.getString("id"), "\"");
        final List<Object> pkValues = Lists.newArrayList(id);

        if (op != Envelope.Operation.CREATE) {
            columnData.put("_id", id);
        }

        RowData rowData = new RowData();
        rowData.setPkNames(Lists.newArrayList(ID_FIELD));
        rowData.setKeys(pkValues);
        rowData.setFields(fields);
        if (op == Envelope.Operation.DELETE) {
            rowData.setBefore(columnData);
        } else {
            rowData.setAfter(columnData);
        }
        rowData.setDatabase(database);
        rowData.setTable(tableName);
        rowData.setOp(op.code());
        rowData.setTs(ts);

        out.collect(rowData);
    }

    @Override
    protected Envelope.Operation extractOperation(Struct value) {
        final Field opField = value.schema().field(OPERATION);
        return Envelope.Operation.forCode(value.getString(opField.name()));
    }

    private Long extractTimestamp(Struct struct) {
        return struct.getInt64(TIMESTAMP);
    }

    @SneakyThrows
    private Map<String, Object> extractColumnData(Envelope.Operation op, Struct value) {
        final String after = value.getString(AFTER);
        final String patch = value.getString(PATCH);
        BsonDocument data = new BsonDocument();
        if (StringUtils.isNotBlank(after)) {
            data.putAll(BsonDocument.parse(after));
        }

        if (StringUtils.isNotBlank(patch)) {
            final BsonDocument patchData = BsonDocument.parse(patch);
            if (patchData.containsKey(SET)) {
                data.putAll((BsonDocument) patchData.get(SET));
            }
            if (patchData.containsKey(UNSET)) {
                data.putAll(createUnsetData((BsonDocument) patchData.get(UNSET)));
            }
        }

        return extractColumns(null, data);
    }

    private BsonDocument createUnsetData(BsonDocument unsetData) {
        BsonDocument unsetTemp = new BsonDocument();
        for (Map.Entry<String, BsonValue> entry : unsetData.entrySet()) {
            if (entry.getValue() instanceof Map) {
                unsetTemp.putAll(createUnsetData((BsonDocument) entry.getValue()));
            } else {
                unsetTemp.put(entry.getKey(), null);
            }
        }
        return unsetTemp;
    }

    @Override
    protected String extractTableName(Struct value) {
        final Struct source = (Struct) value.get(SOURCE);
        return (String) source.get(COLLECTION);
    }

    @Override
    protected String extractDatabase(Struct value) {
        final Struct source = (Struct) value.get(SOURCE);
        return (String) source.get(DB);
    }

}
