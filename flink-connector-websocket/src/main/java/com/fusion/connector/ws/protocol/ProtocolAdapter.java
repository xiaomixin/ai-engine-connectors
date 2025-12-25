package com.fusion.connector.ws.protocol;

import com.fusion.connector.ws.entity.WsEnvelope;

public interface ProtocolAdapter<C> {
    Iterable<String> initialMessages(C config);

    default boolean isControlMessage(String rawMessage) {
        return false;
    }

    WsEnvelope parseEnvelope(String rawJson);
}
