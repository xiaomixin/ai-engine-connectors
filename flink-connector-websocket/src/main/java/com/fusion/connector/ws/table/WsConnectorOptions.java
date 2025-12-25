package com.fusion.connector.ws.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public final class WsConnectorOptions {

    private WsConnectorOptions() {}

    // ---- Required ----
    public static final ConfigOption<String> WS_URL =
            ConfigOptions.key("ws.url")
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<String> WS_SOURCE =
            ConfigOptions.key("ws.source")
                    .stringType()
                    .defaultValue("generic");

    public static final ConfigOption<String> WS_CHANNEL =
            ConfigOptions.key("ws.channel")
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<String> WS_SYMBOL =
            ConfigOptions.key("ws.symbol")
                    .stringType()
                    .noDefaultValue();

    // ---- Backpressure / buffering ----
    public static final ConfigOption<Integer> WS_QUEUE_CAPACITY =
            ConfigOptions.key("ws.queue.capacity")
                    .intType()
                    .defaultValue(50_000);

    public static final ConfigOption<Integer> WS_REQUEST_BATCH =
            ConfigOptions.key("ws.request.batch")
                    .intType()
                    .defaultValue(2_000);

    // ---- Future: reconnect/timeout/format/auth etc. (placeholder) ----
    public static final ConfigOption<Integer> WS_CONNECT_TIMEOUT_MS =
            ConfigOptions.key("ws.connect.timeout.ms")
                    .intType()
                    .defaultValue(10_000);

    public static final ConfigOption<Integer> WS_RECONNECT_MAX_RETRIES =
            ConfigOptions.key("ws.reconnect.max.retries")
                    .intType()
                    .defaultValue(0); // 0 = disabled in v1

    public static final ConfigOption<Integer> WS_RECONNECT_BACKOFF_MS =
            ConfigOptions.key("ws.reconnect.backoff.ms")
                    .intType()
                    .defaultValue(1_000);
}

