package com.fusion.connector.ws.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.*;

import java.math.BigDecimal;

public class JsonTypeConverter {
    public Object toInternal(LogicalType t, JsonNode v) {
        if (v == null || v.isNull() || v.isMissingNode()) {
            return null;
        }

        return switch (t.getTypeRoot()) {
            case CHAR, VARCHAR -> StringData.fromString(asText(v));
            case BOOLEAN -> v.asBoolean();

            case INTEGER -> v.asInt();
            case BIGINT -> v.asLong();
            case FLOAT -> (float) v.asDouble();
            case DOUBLE -> v.asDouble();

            case DECIMAL -> toDecimal((DecimalType) t, v);

            case TIMESTAMP_WITHOUT_TIME_ZONE,
                 TIMESTAMP_WITH_LOCAL_TIME_ZONE -> toTimestamp((TimestampType) t, v);

            // v1: fallback -> store as JSON string
            default -> StringData.fromString(v.toString());
        };
    }

    private String asText(JsonNode v) {
        // numbers -> keep raw string, strings -> text
        if (v.isTextual()) return v.asText();
        return v.asText(); // JsonNode handles numeric -> string too
    }

    private DecimalData toDecimal(DecimalType dt, JsonNode v) {
        final int p = dt.getPrecision();
        final int s = dt.getScale();

        BigDecimal bd;
        if (v.isNumber()) {
            bd = v.decimalValue();
        } else {
            // Hyperliquid often encodes numbers as string
            bd = new BigDecimal(v.asText());
        }
        // normalize scale if needed
        bd = bd.setScale(s, BigDecimal.ROUND_HALF_UP);
        return DecimalData.fromBigDecimal(bd, p, s);
    }

    private TimestampData toTimestamp(TimestampType tt, JsonNode v) {
        long epochMillis;
        if (v.isNumber()) {
            epochMillis = v.asLong();
        } else {
            epochMillis = Long.parseLong(v.asText());
        }
        return TimestampData.fromEpochMillis(epochMillis);
    }
}
