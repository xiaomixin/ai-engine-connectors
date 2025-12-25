package com.fusion.connector.ws.protocol;

import com.fasterxml.jackson.databind.JsonNode;
import com.fusion.connector.ws.entity.WsTableConfig;
import com.fusion.connector.ws.source.NormalizedEvent;

import java.util.List;

public class L2BookDecoder implements ChannelDecoder<WsTableConfig> {
    @Override
    public String channel() {
        return "l2Book";
    }

    @Override
    public Iterable<NormalizedEvent> decode(WsTableConfig cfg, JsonNode data) {
        JsonNode coinNode = data.path("coin");
        String coin = (coinNode != null && coinNode.isTextual()) ? coinNode.asText() : cfg.symbol();
        JsonNode timeNode = data.path("time");
        long ts = (timeNode != null && timeNode.isNumber()) ? timeNode.asLong() : System.currentTimeMillis();
        String payload = data.toString();
        return List.of(new NormalizedEvent(
                cfg.source(),
                channel(),
                coin,
                ts,
                payload
        ));
    }
}
