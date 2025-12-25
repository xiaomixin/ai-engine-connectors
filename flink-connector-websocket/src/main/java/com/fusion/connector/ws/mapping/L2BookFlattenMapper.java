package com.fusion.connector.ws.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;

public class L2BookFlattenMapper implements ChannelRowMapper {
    private final RowType rowType;
    private final JsonTypeConverter converter;

    public L2BookFlattenMapper(RowType rowType, JsonTypeConverter converter) {
        this.rowType = rowType;
        this.converter = converter;
    }

    @Override
    public String channel() {
        return "l2Book";
    }

    @Override
    public List<RowData> map(JsonNode data) {
        if(data == null || data.isNull() || data.isMissingNode()) return List.of();
        JsonNode levels = data.get("levels");
        if(levels == null || !levels.isArray() || levels.size() < 2) return List.of();
        JsonNode bids = levels.get(0);
        JsonNode asks = levels.get(1);
        List<RowData> out = new ArrayList<>();
        // bids: side=BID
        flattenSide(out, data, "BID", bids);
        // asks: side=ASK
        flattenSide(out, data, "ASK", asks);
        return out;
    }

    private void flattenSide(List<RowData> out, JsonNode data, String side, JsonNode sideLevels) {
        if(sideLevels == null || !sideLevels.isArray()) return;
        int k = sideLevels.size();
        for (int i = 0; i < k; i++) {
            JsonNode level = sideLevels.get(i);
            GenericRowData row = new GenericRowData(rowType.getFieldCount());

            for (int c = 0; c < rowType.getFieldCount(); c++) {
                String col = rowType.getFieldNames().get(c);
                LogicalType t = rowType.getTypeAt(c);
                JsonNode v = switch (col) {
                    case "coin" -> data.get("coin");
                    case "event_time" -> data.get("time");
                    case "side" -> textNode(side);
                    case "level_idx" -> intNode(i + 1);
                    case "px" -> extractPx(level);
                    case "sz" -> extractSz(level);
                    case "n" -> extractN(level);
                    default -> null; // ignore unknown columns in this mapper
                };

                row.setField(c, converter.toInternal(t, v));
            }
            out.add(row);
        }
    }

    private JsonNode extractPx(JsonNode level) {
        if(level == null) return null;
        if(level.isArray() && !level.isEmpty()) return level.get(0);
        return level.get("px");
    }

    private JsonNode extractSz(JsonNode level) {
        if(level == null) return null;
        if(level.isArray() && level.size() > 1) return level.get(1);
        return level.get("sz");
    }

    private JsonNode extractN(JsonNode level) {
        if(level == null) return null;
        if(level.isArray() && level.size() > 2) return level.get(2);
        return level.get("n");
    }

    private static JsonNode textNode(String s) {
        return com.fasterxml.jackson.databind.node.TextNode.valueOf(s);
    }

    private static JsonNode intNode(int v) {
        return new com.fasterxml.jackson.databind.node.IntNode(v);
    }
}
