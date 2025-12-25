package com.fusion.connector.ws.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;

public final class TradesMapper implements ChannelRowMapper {
    private final RowType rowType;
    private final JsonTypeConverter converter;

    public TradesMapper(RowType rowType, JsonTypeConverter converter) {
        this.rowType = rowType;
        this.converter = converter;
    }

    @Override
    public String channel() {
        return "trades";
    }

    @Override
    public List<RowData> map(JsonNode data) {
        if (data == null || data.isNull() || data.isMissingNode()) return List.of();

        // data may be an object or an array
        if (data.isArray()) {
            List<RowData> out = new ArrayList<>();
            for (JsonNode item : data) {
                RowData r = mapOne(item);
                if (r != null) out.add(r);
            }
            return out;
        }

        RowData one = mapOne(data);
        return one == null ? List.of() : List.of(one);
    }

    private RowData mapOne(JsonNode trade) {
        if (trade == null || trade.isNull() || trade.isMissingNode()) return null;

        GenericRowData row = new GenericRowData(rowType.getFieldCount());
        for (int c = 0; c < rowType.getFieldCount(); c++) {
            String col = rowType.getFieldNames().get(c);
            LogicalType t = rowType.getTypeAt(c);

            JsonNode v = switch (col) {
                case "coin" -> trade.get("coin");
                case "event_time" -> trade.get("time");
                case "side" -> trade.get("side");
                case "px" -> trade.get("px");
                case "sz" -> trade.get("sz");
                default -> null;
            };

            row.setField(c, converter.toInternal(t, v));
        }
        return row;
    }
}
