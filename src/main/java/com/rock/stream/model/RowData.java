package com.rock.stream.model;

import lombok.Data;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Data
public class RowData implements Serializable {
    private String database;
    private String table;
    private String op;
    private Long ts;
    private List<String> pkNames;
    private List<Object> keys;
    private List<Field> fields;
    private Map<String, Object> after;
    private Map<String, Object> before;

    public String keyStrValues() {
        return keys.stream().map(String::valueOf).collect(Collectors.joining("$"));
    }

    public List<String> getPkNames() {
        return Optional.ofNullable(pkNames)
                .orElse(Collections.emptyList());
    }

    public List<Object> getKeys() {
        return Optional.ofNullable(keys)
                .orElse(Collections.emptyList());
    }

    public List<Field> getFields() {
        return Optional.ofNullable(fields)
                .orElse(Collections.emptyList());
    }

    public Map<String, Object> getAfter() {
        return Optional.ofNullable(after).orElse(Collections.emptyMap());
    }

    public Map<String, Object> getBefore() {
        return Optional.ofNullable(before).orElse(Collections.emptyMap());
    }

    @Data
    public static class Field implements Serializable {
        private String type;
        private boolean optional;
        private String field;
    }
}
