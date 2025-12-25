package com.fusion.connector.ws.protocol;

import com.fasterxml.jackson.databind.JsonNode;
import com.fusion.connector.ws.source.NormalizedEvent;

public interface ChannelDecoder<C> {
    String channel();

    Iterable<NormalizedEvent> decode(C cfg, JsonNode data);
}
