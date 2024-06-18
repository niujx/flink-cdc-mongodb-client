package com.rock.stream;

import com.rock.stream.model.RowData;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface SyncInstance {

    SingleOutputStreamOperator<RowData> process() throws Exception;

    String name();

    void init();


}
