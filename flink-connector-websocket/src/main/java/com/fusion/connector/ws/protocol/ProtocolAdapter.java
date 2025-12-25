package com.fusion.connector.ws.protocol;

import com.fusion.connector.ws.source.NormalizedEvent;

public interface ProtocolAdapter<C> {

    Iterable<String> initialMessages(C config);

    Iterable<NormalizedEvent> decode(C config, String rawMessage);

    default boolean isControlMessage(String rawMessage) {
        return false;
    }
}
