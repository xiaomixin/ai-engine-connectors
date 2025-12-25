package com.fusion.connector.ws.mapping;

import org.apache.flink.table.types.logical.RowType;

public class RowMapperFactory {

    private RowMapperFactory() {
    }

    public static ChannelRowMapper create(String wsChannel, RowType rowType) {
        JsonTypeConverter converter = new JsonTypeConverter();
        if(wsChannel == null || wsChannel.isEmpty()){
            return new GenericJsonProjectMapper(rowType, converter);
        }
        return switch (wsChannel) {
            case "trades" -> new TradesMapper(rowType, converter);
            case "l2Book" -> new L2BookFlattenMapper(rowType, converter);
            default -> new GenericJsonProjectMapper(rowType, converter);
        };
    }
}
