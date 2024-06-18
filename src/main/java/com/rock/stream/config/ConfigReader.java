package com.rock.stream.config;

import com.rock.stream.model.DBType;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.Properties;

public class ConfigReader extends BaseConfigReader {

    public static final String CDC_COLLECTOR_DATABASE_TYPE = "cdc.collector.database.type";

    public static final String CDC_TASK_NAME = "cdc.collector.task.name";

    public static final String CDC_MYSQL_HOSTNAME = "cdc.collector.mysql.hostname";

    public static final String CDC_MYSQL_PORT = "cdc.collector.mysql.port";

    public static final String CDC_MYSQL_USERNAME = "cdc.collector.mysql.username";

    public static final String CDC_MYSQL_PASSWORD = "cdc.collector.mysql.password";

    public static final String CDC_MONGODB_HOSTS = "cdc.collector.mongodb.hosts";

    public static final String CDC_MONGODB_PORT = "cdc.collector.mongodb.port";

    public static final String CDC_MONGODB_USERNAME = "cdc.collector.mongodb.username";

    public static final String CDC_MONGODB_PASSWORD = "cdc.collector.mongodb.password";

    public static final String CDC_MONGODB_CONNECTOR_TYPE = "cdc.collector.mongodb.type";

    public static final String CDC_DATABASE_LIST = "cdc.collector.database-list";

    public static final String CDC_TABLE_LIST = "cdc.collector.table-list";

    public static final String CDC_TIMEZONE = "cdc.collector.timezone";

    public static final String CDC_STARTUP_MODE = "cdc.collector.startup.mode";

    public static final String CDC_STARTUP_OFFSET_FILE = "cdc.collector.startup.specific-offset.file";

    public static final String CDC_STARTUP_OFFSET_POS = "cdc.collector.startup.specific-offset.pos";

    public static final String CDC_STARTUP_TIMESTAMP = "cdc.collector.startup.specific-timestamp.millis";

    public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.producer.servers";
    public static final String KAFKA_PRODUCER_PREFIX = "kafka.producer.prefix";

    public ConfigReader(Properties properties) {
        super(properties);
    }

    public DBType getCdcCollectorDatabaseType() {
        final String databaseType = properties.getProperty(CDC_COLLECTOR_DATABASE_TYPE);
        return DBType.valueOf(databaseType);
    }

    public String getCdcTaskName() {
        return properties.getProperty(CDC_TASK_NAME);
    }

    public String getCdcMysqlHostname() {
        return properties.getProperty(CDC_MYSQL_HOSTNAME);
    }

    public Integer getCdcMysqlPort() {
        return NumberUtils.toInt(properties.getProperty(CDC_MYSQL_PORT), 3306);
    }

    public String getCdcMysqlUsername() {
        return properties.getProperty(CDC_MYSQL_USERNAME);
    }

    public String getCdcMysqlPassword() {
        return properties.getProperty(CDC_MYSQL_PASSWORD);
    }

    public String getCdcMongodbHosts() {
        return properties.getProperty(CDC_MONGODB_HOSTS);
    }

    public Integer getCdcMongodbPort() {
        return NumberUtils.toInt(properties.getProperty(CDC_MONGODB_PORT));
    }

    public String getCdcMongodbUsername() {
        return properties.getProperty(CDC_MONGODB_USERNAME);
    }

    public String getCdcMongodbPassword() {
        return properties.getProperty(CDC_MONGODB_PASSWORD);
    }

    public String getCdcMongodbConnectorType() {
        return properties.getProperty(CDC_MONGODB_CONNECTOR_TYPE, "oplog");
    }

    public String getCdcDatabaseList() {
        return properties.getProperty(CDC_DATABASE_LIST);
    }

    public String getCdcTableList() {
        return properties.getProperty(CDC_TABLE_LIST);
    }

    public String getCdcTimezone() {
        return properties.getProperty(CDC_TIMEZONE);
    }

    public String getCdcStartupMode() {
        return properties.getProperty(CDC_STARTUP_MODE);
    }

    public String getCdcStartupOffsetFile() {
        return properties.getProperty(CDC_STARTUP_OFFSET_FILE);
    }

    public String getCdcStartupOffsetPos() {
        return properties.getProperty(CDC_STARTUP_OFFSET_POS);
    }

    public String getCdcStartUpTimestamp() {
        return properties.getProperty(CDC_STARTUP_TIMESTAMP);
    }

    public String getKafkaProducerPrefix() {
        return properties.getProperty(KAFKA_PRODUCER_PREFIX);
    }

    public String getKafkaBootstrapServers() {
        return properties.getProperty(KAFKA_BOOTSTRAP_SERVERS);
    }

}
