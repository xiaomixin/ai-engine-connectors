package com.fusion.connector.ws.protocol;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fusion.connector.ws.entity.UnknownChannelPolicy;
import com.fusion.connector.ws.entity.WsEnvelope;
import com.fusion.connector.ws.entity.WsTableConfig;
import com.fusion.connector.ws.source.NormalizedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HyperliquidWsAdapter implements ProtocolAdapter<WsTableConfig> {
    private static final Logger LOG = LoggerFactory.getLogger(HyperliquidWsAdapter.class);
    private final ObjectMapper objectMapper;

    private final DecoderRegistry<WsTableConfig> registry;

    private final UnknownChannelPolicy unknownPolicy;

    public HyperliquidWsAdapter(DecoderRegistry<WsTableConfig> registry,
                                UnknownChannelPolicy unknownPolicy) {
        this.registry = registry;
        this.unknownPolicy = unknownPolicy;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public Iterable<String> initialMessages(WsTableConfig config) {
        if(!"l2Book".equals(config.channel())){
            return List.of();
        }
        return List.of(HyperliquidSubscribeBuilder.buildL2BookSubscribe(config));
    }

    public WsEnvelope parseEnvelope(String message) throws Exception {
        JsonNode root = objectMapper.readTree(message);
        JsonNode channelNode = root.get("channel");
        String channel = (channelNode != null && channelNode.isTextual()) ? channelNode.asText() : null;
        JsonNode data = root.get("data");
        return new WsEnvelope(channel, data);
    }

    @Override
    public Iterable<NormalizedEvent> decode(WsTableConfig cfg, String rawMessage) {
        try {
            WsEnvelope envelope = parseEnvelope(rawMessage);
            if(envelope.channel() == null || envelope.channel().isEmpty()){
                return List.of();
            }
            if("subscriptionResponse".equals(envelope.channel())){
                return List.of();
            }
            ChannelDecoder<WsTableConfig> decoder = registry.get(envelope.channel());
            if(decoder == null){
                return switch (unknownPolicy) {
                    case DROP -> List.of();
                    case PASS_THROUGH -> List.of(
                            new NormalizedEvent(
                                    cfg.source(),
                                    envelope.channel(),
                                    "",
                                    System.currentTimeMillis(),
                                    rawMessage
                            )
                    );
                };
            }
            return decoder.decode(cfg, envelope.data());
        } catch(Exception e) {
            LOG.error(e.getMessage(), e);
            return List.of();
        }
    }
}
