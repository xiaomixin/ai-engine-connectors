package com.fusion.connector.ws.source;

public record NormalizedEvent(
        String source,
        String channel,
        String symbol,
        long eventTimeMs,
        String payloadJson
) {}
