package com.rock.stream.mongodb;

import com.rock.stream.config.ConfigReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

public class MongodbInstanceConfigFactory {


    public MongodbInstanceConfig getInstanceConfig(ConfigReader configReader) {
        final String startupTimestampMillisValue = configReader.getCdcStartUpTimestamp();
        Long startupTimestampMillis = null;
        if (StringUtils.isNotBlank(startupTimestampMillisValue) && NumberUtils.isCreatable(startupTimestampMillisValue)) {
            startupTimestampMillis = NumberUtils.toLong(startupTimestampMillisValue);
        }
        return MongodbInstanceConfig.newBuilder()
                .setName(configReader.getCdcTaskName())
                .setHosts(configReader.getCdcMongodbHosts())
                .setUsername(configReader.getCdcMongodbUsername())
                .setPassword(configReader.getCdcMongodbPassword())
                .setDatabaseList(configReader.getCdcDatabaseList())
                .setCollectionList(configReader.getCdcTableList())
                .setType(configReader.getCdcMongodbConnectorType())
                .setTimezone(configReader.getCdcTimezone())
                .setStartupMode(configReader.getCdcStartupMode())
                .setStartupTimestampMillis(startupTimestampMillis)
                .build();
    }
}