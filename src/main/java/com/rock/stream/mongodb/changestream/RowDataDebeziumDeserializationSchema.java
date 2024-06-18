package com.rock.stream.mongodb.changestream;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.rock.stream.model.RowData;
import com.rock.stream.BaseDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope;
import io.debezium.data.Envelope;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

public class RowDataDebeziumDeserializationSchema extends BaseDebeziumDeserializationSchema {

    protected final TimeZone localTimeZone;
    protected static final String DATE_FORMAT_PATTERN = "yyyy-MM-dd HH:mm:ss";

    public RowDataDebeziumDeserializationSchema(String timeZone) {
        localTimeZone = TimeZone.getTimeZone(timeZone);
    }

    @Override
    public void deserialize(SourceRecord record, Collector<RowData> out) throws Exception {
        try {
            final Struct value = (Struct) record.value();

            String tableName = extractTableName(value);
            String database = extractDatabase(value);

            BsonDocument keyDocument = Preconditions.checkNotNull(extractBsonDocument(value, MongoDBEnvelope.DOCUMENT_KEY_FIELD));
            BsonDocument fullDocument = extractBsonDocument(value, MongoDBEnvelope.FULL_DOCUMENT_FIELD);

            List<String> pkNames = Lists.newArrayList(MongoDBEnvelope.ID_FIELD);
            List<Object> pkValues = Lists.newArrayList(toValue(keyDocument.get(MongoDBEnvelope.ID_FIELD)).toString());
            Envelope.Operation op = extractOperation(value);
            long ts = 0L;
            if (op == null) return;

            RowData rowData = new RowData();
            rowData.setPkNames(pkNames);
            rowData.setKeys(pkValues);
            if (op != Envelope.Operation.DELETE) {
                List<RowData.Field> fields = extractFields(value);
                rowData.setFields(fields);
                rowData.setAfter(extractColumnData(fullDocument));
            }
            rowData.setDatabase(database);
            rowData.setTable(tableName);
            rowData.setOp(op.code());
            rowData.setTs(ts);
            out.collect(rowData);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    protected String extractTableName(Struct value) {
        final Struct ns = (Struct) value.get(MongoDBEnvelope.NAMESPACE_FIELD);
        return (String) ns.get(MongoDBEnvelope.NAMESPACE_COLLECTION_FIELD);
    }

    protected String extractDatabase(Struct value) {
        final Struct ns = (Struct) value.get(MongoDBEnvelope.NAMESPACE_FIELD);
        return (String) ns.get(MongoDBEnvelope.NAMESPACE_DATABASE_FIELD);
    }

    protected Envelope.Operation extractOperation(Struct value) {
        final Field opField = value.schema().field(MongoDBEnvelope.OPERATION_TYPE_FIELD);
        final String code = value.getString(opField.name());
        String debeziumOp = null;
        switch (code) {
            case "insert":
                debeziumOp = "c";
                break;
            case "delete":
                debeziumOp = "d";
                break;
            case "update":
            case "replace":
                debeziumOp = "u";
                break;
            default:
        }
        return Envelope.Operation.forCode(debeziumOp);
    }

    protected Map<String, Object> extractColumnData(BsonDocument fullDocument) {
        return extractColumns(null, fullDocument);
    }

    protected Map<String, Object> extractColumns(String prefix, BsonDocument document) {
        Map<String, Object> columns = Maps.newHashMap();
        for (Map.Entry<String, BsonValue> column : document.entrySet()) {
            final String fieldName = column.getKey();
            final BsonValue value = column.getValue();
            final String fullFieldName = Optional.ofNullable(prefix).map(p -> p + "." + fieldName).orElse(fieldName);
            Object o = toValue(value);
            if (o instanceof BsonDocument) {
                columns.putAll(extractColumns(fullFieldName, (BsonDocument) o));
            } else {
                columns.put(fullFieldName, o);
            }
        }
        return columns;
    }

    @SneakyThrows
    public Object toValue(BsonValue bsonValue) {
        switch (bsonValue.getBsonType()) {
            case INT32:
                return bsonValue.asInt32().getValue();
            case INT64:
                return bsonValue.asInt64().getValue();
            case ARRAY:
                final List<Object> collect = new ArrayList<>();
                for (BsonValue bsonValue1 : bsonValue.asArray()) {
                    Object o;
                    if (bsonValue1 instanceof BsonDocument) {
                        o = extractColumns(null, bsonValue1.asDocument());
                    }else{
                        o = toValue(bsonValue1);
                    }
                    collect.add(o);
                }
                return new ObjectMapper().writeValueAsString(collect);
            case DOCUMENT:
                return bsonValue.asDocument().toJson();
            case DATE_TIME:
                final long value = bsonValue.asDateTime().getValue();
                return DateFormatUtils.format(new Date(value), DATE_FORMAT_PATTERN, localTimeZone);
            case TIMESTAMP:
                return DateFormatUtils.format(new Date(bsonValue.asTimestamp().getTime() * 1000L), DATE_FORMAT_PATTERN, localTimeZone);
            case DOUBLE:
                return bsonValue.asDouble().getValue();
            case OBJECT_ID:
                return bsonValue.asObjectId().getValue().toString();
            case BOOLEAN:
                return bsonValue.asBoolean().getValue();
            case NULL:
                return null;
            case DECIMAL128:
                return bsonValue.asDecimal128().getValue().toString();
            case STRING:
            case BINARY:
            default:
                return bsonValue.asString().getValue();
        }
    }

    protected BsonDocument extractBsonDocument(Struct value, String fieldName) {
        final String json = value.getString(fieldName);
        return StringUtils.isNotBlank(json) ?
                BsonDocument.parse(json) : null;
    }

    protected List<RowData.Field> extractFields(Struct value) {
        return null;
    }

}
