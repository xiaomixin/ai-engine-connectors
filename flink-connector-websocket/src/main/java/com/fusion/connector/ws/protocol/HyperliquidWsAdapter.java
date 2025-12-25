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
        HyperliquidSubscriptionType subscriptionType = HyperliquidSubscriptionType.fromChannel(config.channel());
        if (subscriptionType == null) {
            LOG.warn("Unsupported channel type: {}", config.channel());
            return List.of();
        }
        return List.of(buildSubscribeMessage(subscriptionType, config.symbol()));
    }

    private String buildSubscribeMessage(HyperliquidSubscriptionType subscriptionType, String coin) {
        ObjectNode root = objectMapper.createObjectNode();
        root.put("method", "subscribe");
        ObjectNode subscription = root.putObject("subscription");
        subscription.put("type", subscriptionType.getTypeValue());
        subscription.put("coin", coin);
        return root.toString();
    }

    @Override
    public WsEnvelope parseEnvelope(String rawJson) {
        try {
            JsonNode root = objectMapper.readTree(rawJson);
            JsonNode channelNode = root.get("channel");
            String channel = (channelNode != null && channelNode.isTextual()) ? channelNode.asText() : null;
            JsonNode data = root.get("data");
            return new WsEnvelope(channel, data);
        } catch (JsonProcessingException e) {
            LOG.error("Failed to parse WebSocket message: {}", rawJson, e);
            return null;
        }
    }
}
