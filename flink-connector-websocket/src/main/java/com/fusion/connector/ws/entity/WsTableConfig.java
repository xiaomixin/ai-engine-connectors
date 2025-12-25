package com.fusion.connector.ws.entity;

import java.io.Serial;
import java.io.Serializable;

public record WsTableConfig(String url,
                            String source,
                            String channel,
                            String symbol,
                            int queueCapacity,
                            int requestBatch) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public String toString() {
        return "WsTableConfig{" +
                "url='" + url + '\'' +
                ", source='" + source + '\'' +
                ", channel='" + channel + '\'' +
                ", symbol='" + symbol + '\'' +
                ", queueCapacity=" + queueCapacity +
                ", requestBatch=" + requestBatch +
                '}';
    }
}
