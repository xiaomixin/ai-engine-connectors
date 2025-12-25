package com.fusion.connector.ws.mapping;

import com.fusion.connector.ws.protocol.HyperliquidSubscriptionType;
import org.apache.flink.table.types.logical.RowType;

import static com.fusion.connector.ws.protocol.HyperliquidSubscriptionType.HP_TRADES;

public class RowMapperFactory {

    private RowMapperFactory() {
    }

    public static ChannelRowMapper create(String wsChannel, RowType rowType) {
        JsonTypeConverter converter = new JsonTypeConverter();
        if(wsChannel == null || wsChannel.isEmpty()){
            return new GenericJsonProjectMapper(rowType, converter);
        }
        return switch (HyperliquidSubscriptionType.fromChannel(wsChannel)) {
            case HP_TRADES -> new TradesMapper(rowType, converter);
            case HP_L2_BOOK -> new L2BookFlattenMapper(rowType, converter);
        };
    }
}
