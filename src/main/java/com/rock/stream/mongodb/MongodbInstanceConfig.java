package com.rock.stream.mongodb;

import com.ververica.cdc.connectors.base.options.StartupOptions;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

@Getter
public class MongodbInstanceConfig {

    private final String hosts;

    private final String username;

    private final String password;

    private final String[] databaseList;

    private final String[] collectionList;

    public final String name;

    private final String timezone;

    private final String type;

    private final StartupOptions startupOptions;


    public MongodbInstanceConfig(String name,
                                 String hosts,
                                 String username,
                                 String password,
                                 String[] databaseList,
                                 String[] collectionList,
                                 String type,
                                 String timezone,
                                 StartupOptions startupOptions) {
        this.name = name;
        this.hosts = hosts;
        this.username = username;
        this.password = password;
        this.databaseList = databaseList;
        this.collectionList = collectionList;
        this.type = type;
        this.timezone = timezone;
        this.startupOptions =startupOptions;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private String hosts;
        private String username;
        private String password;
        private String databaseList;
        private String collectionList;
        private String type;
        private String timezone;

        private String startupMode;

        private Long startupTimestampMillis;
        private static final String DEFAULT_TIME_ZONE = "Asia/Shanghai";
        private static final String DEFAULT_CDC_NAME = "ods-cdc-source";

        private String name;

        public Builder setHosts(String hosts) {
            this.hosts = hosts;
            return this;
        }

        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setDatabaseList(String databaseList) {
            this.databaseList = databaseList;
            return this;
        }

        public Builder setCollectionList(String collectionList) {
            this.collectionList = collectionList;
            return this;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setType(String type) {
            this.type = type;
            return this;
        }

        public Builder setTimezone(String timezone) {
            this.timezone = timezone;
            return this;

        }

        public Builder setStartupMode(String startupMode) {
            this.startupMode = startupMode;
            return this;
        }

        public Builder setStartupTimestampMillis(Long startupTimestampMillis) {
            this.startupTimestampMillis = startupTimestampMillis;
            return this;
        }

        private StartupOptions startupOptions() {

            if (StringUtils.equalsIgnoreCase(startupMode, "earliest")) {
                return StartupOptions.earliest();
            }

            if (StringUtils.equalsIgnoreCase(startupMode, "initial")) {
                return StartupOptions.initial();
            }

            if (StringUtils.equalsIgnoreCase(startupMode, "timestamp")) {
                checkState(startupTimestampMillis != null && startupTimestampMillis > 0, "startupTimestampMillis must gte zero.");
                return StartupOptions.timestamp(startupTimestampMillis);
            }

            //默认是取最新的binlog数据
            return StartupOptions.latest();

        }

        public MongodbInstanceConfig build() {

            checkState(StringUtils.isNotBlank(hosts), "db hosts is not null");
            checkState(StringUtils.isNotBlank(username), "db user name is not null");
            checkState(StringUtils.isNotBlank(password), "password is not null");
            checkState(StringUtils.isNotBlank(type) && Arrays.asList("oplog", "changeStream").contains(type), "type must set oplog or changeStream");


            name = Optional.ofNullable(name).orElse(DEFAULT_CDC_NAME);
            timezone = Optional.ofNullable(timezone).orElse(DEFAULT_TIME_ZONE);

            String[] databases = StringUtils.split(databaseList, ",");
            String[] collections = StringUtils.split(collectionList, ",");

            return new MongodbInstanceConfig(
                    name,
                    hosts,
                    username,
                    password,
                    databases,
                    collections,
                    type,
                    timezone,
                    startupOptions()

            );


        }
    }

}
