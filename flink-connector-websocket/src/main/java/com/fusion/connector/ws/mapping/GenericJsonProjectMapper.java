package com.fusion.connector.ws.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

public class GenericJsonProjectMapper implements ChannelRowMapper {
    private final RowType rowType;
    private final JsonTypeConverter converter;

    public GenericJsonProjectMapper(RowType rowType, JsonTypeConverter converter) {
        this.rowType = rowType;
        this.converter = converter;
    }

    @Override
    public String channel() {
        return "*";
    }

    @Override
    public List<RowData> map(JsonNode data) {
        GenericRowData row = new GenericRowData(rowType.getFieldCount());

        for (int i = 0; i < rowType.getFieldCount(); i++) {
            String col = rowType.getFieldNames().get(i);
            LogicalType t = rowType.getTypeAt(i);

            JsonNode v = extractByColumnName(data, col);
            Object internal = converter.toInternal(t, v);
            row.setField(i, internal);
        }
        return List.of(row);
    }

    /**
     * Column extraction rules:
     * 1) direct key match: data.get(col)
     * 2) support dotted path: a.b.c
     * 3) special mapping: event_time -> data.time (epoch millis)
     */
    private JsonNode extractByColumnName(JsonNode data, String col) {
        if(data == null || data.isMissingNode() || data.isNull()) return null;

        // special: event_time comes from time
        if("event_time".equals(col)){
            JsonNode time = data.get("time");
            return time;
        }

        // dotted path
        if(col.contains(".")){
            String[] parts = col.split("\\.");
            JsonNode cur = data;
            for (String p : parts) {
                if(cur == null) return null;
                cur = cur.get(p);
            }
            return cur;
        }

        return data.get(col);
    }
}
