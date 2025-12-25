package com.fusion.connector.ws.protocol;

import com.fusion.connector.ws.entity.WsTableConfig;
import com.fusion.connector.ws.source.NormalizedEvent;

import java.util.List;

public class PassthroughAdapter implements ProtocolAdapter<WsTableConfig> {
    @Override
    public Iterable<String> initialMessages(WsTableConfig config) {
        return List.of();
    }

    @Override
    public Iterable<NormalizedEvent> decode(WsTableConfig config, String rawMessage) {
        long currentTs = System.currentTimeMillis();
        return List.of(new NormalizedEvent(
                config.source(),
                config.channel(),
                config.symbol(),
                currentTs,
                rawMessage
        ));
    }

    @Override
    public boolean isControlMessage(String rawMessage) {
        return ProtocolAdapter.super.isControlMessage(rawMessage);
    }
}
