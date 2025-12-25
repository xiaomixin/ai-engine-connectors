package com.fusion.connector.ws.protocol;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fusion.connector.ws.entity.WsEnvelope;
import com.fusion.connector.ws.entity.WsTableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HyperliquidWsAdapter implements ProtocolAdapter<WsTableConfig> {
    private static final Logger LOG = LoggerFactory.getLogger(HyperliquidWsAdapter.class);
    private final ObjectMapper objectMapper;

    public HyperliquidWsAdapter() {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public Iterable<String> initialMessages(WsTableConfig config) {
        if(!"l2Book".equals(config.channel())){
            return List.of();
        }
        return List.of(buildL2BookInitMessage(config));
    }

    public String buildL2BookInitMessage(WsTableConfig cfg) {
        ObjectNode root = objectMapper.createObjectNode();
        root.put("method", "subscribe");
        ObjectNode sub = root.putObject("subscription");
        sub.put("type", "l2Book");
        sub.put("coin", cfg.symbol());
        return root.toString();
    }

    public WsEnvelope parseEnvelope(String message) {
        try {
            JsonNode root = objectMapper.readTree(message);
            JsonNode channelNode = root.get("channel");
            String channel = (channelNode != null && channelNode.isTextual()) ? channelNode.asText() : null;
            JsonNode data = root.get("data");
            return new WsEnvelope(channel, data);
        } catch(JsonProcessingException e) {
            LOG.error(e.getMessage(), e);
            return null;
        }
    }
}
