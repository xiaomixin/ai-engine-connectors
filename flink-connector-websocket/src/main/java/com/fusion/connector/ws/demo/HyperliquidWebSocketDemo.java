package com.fusion.connector.ws.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class HyperliquidWebSocketDemo {

    private static final Logger LOG = LoggerFactory.getLogger(HyperliquidWebSocketDemo.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String WS_URL = "wss://api.hyperliquid.xyz/ws";

    public static void main(String[] args) throws Exception {
        LOG.info("Connecting to Hyperliquid WebSocket: {}", WS_URL);

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        CountDownLatch messageLatch = new CountDownLatch(3);

        WebSocket webSocket = client.newWebSocketBuilder()
                .buildAsync(URI.create(WS_URL), new WebSocket.Listener() {
                    @Override
                    public void onOpen(WebSocket webSocket) {
                        LOG.info("Connection opened");
                        webSocket.request(1);
                    }

                    @Override
                    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                        LOG.info("Received: {}", data);
                        messageLatch.countDown();
                        webSocket.request(1);
                        return WebSocket.Listener.super.onText(webSocket, data, last);
                    }

                    @Override
                    public void onError(WebSocket webSocket, Throwable error) {
                        LOG.error("WebSocket error", error);
                    }
                })
                .join();

        String subscribeMessage = buildSubscribeMessage("trades", "BTC");
        LOG.info("Sending subscription: {}", subscribeMessage);
        webSocket.sendText(subscribeMessage, true);

        boolean received = messageLatch.await(10, TimeUnit.SECONDS);
        LOG.info("Received messages: {}", received);

        webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "Done");
    }

    private static String buildSubscribeMessage(String type, String symbol) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("method", "subscribe");
        ObjectNode sub = root.putObject("subscription");
        sub.put("type", type);
        sub.put("coin", symbol);
        return root.toString();
    }
}
