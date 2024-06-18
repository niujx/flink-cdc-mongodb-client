package com.rock.stream;

import com.rock.stream.model.RowData;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public abstract class BaseDebeziumDeserializationSchema implements DebeziumDeserializationSchema<RowData> {


    @Override
    public TypeInformation<RowData> getProducedType() {
        return TypeInformation.of(RowData.class);
    }

}
