package com.fusion.connector.ws.protocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fusion.connector.ws.entity.WsTableConfig;

public class HyperliquidSubscribeBuilder {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static String buildL2BookSubscribe(WsTableConfig cfg) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("method", "subscribe");
        ObjectNode sub = root.putObject("subscription");
        sub.put("type", "l2Book");
        sub.put("coin", cfg.symbol());
        return root.toString();
    }
}
