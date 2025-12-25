package com.fusion.connector.ws.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

public class JdkHttpClientWsEngine implements WsClientEngine {
    private static final Logger LOG = LoggerFactory.getLogger(JdkHttpClientWsEngine.class);
    private final URI uri;
    private final HttpClient client;
    private final Duration connectTimeout;

    private volatile WsMessageListener listener;
    private volatile WebSocket ws;
    private final AtomicBoolean open = new AtomicBoolean(false);

    public JdkHttpClientWsEngine(String wsUrl, int connectTimeoutMs) {
        this.uri = URI.create(Objects.requireNonNull(wsUrl, "wsUrl"));
        this.connectTimeout = Duration.ofMillis(Math.max(1000, connectTimeoutMs));
        this.client = HttpClient.newBuilder()
                .connectTimeout(this.connectTimeout)
                .build();
    }

    @Override
    public void setListener(WsMessageListener listener) {
        this.listener = listener;
    }

    @Override
    public void close() {
        open.set(false);
        WebSocket s = ws;
        if(s != null){
            try {
                s.sendClose(WebSocket.NORMAL_CLOSURE, "bye").join();
            } catch(Exception ignored) {
                LOG.warn("Error while closing WebSocket", ignored);
            }
        }
    }

    @Override
    public void connect() {
        client.newWebSocketBuilder()
                .buildAsync(uri, new WebSocket.Listener() {

                    @Override
                    public void onOpen(WebSocket webSocket) {
                        ws = webSocket;
                        open.set(true);
                        WsMessageListener l = listener;
                        if(l != null) l.onOpen();
                        // do not auto-request; Source drives request(n)
                        WebSocket.Listener.super.onOpen(webSocket);
                    }

                    @Override
                    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                        WsMessageListener l = listener;
                        if(l != null){
                            if(!last){
                                // Simplest: ignore fragments for now (extend later)
                                System.err.println("[ws] fragmented message ignored (last=false)");
                            } else {
                                l.onText(data.toString(), true);
                            }
                        }
                        return WebSocket.Listener.super.onText(webSocket, data, last);
                    }

                    @Override
                    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                        open.set(false);
                        WsMessageListener l = listener;
                        if(l != null) l.onClosed(statusCode, reason);
                        return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
                    }

                    @Override
                    public void onError(WebSocket webSocket, Throwable error) {
                        WsMessageListener l = listener;
                        if(l != null) l.onError(error);
                    }
                })
                .whenComplete((webSocket, ex) -> {
                    if(ex != null){
                        WsMessageListener l = listener;
                        if(l != null){
                            l.onError(ex);
                            l.onClosed(1006, "connect failed: " + ex.getMessage());
                        }
                    }
                });
    }

    @Override
    public boolean isOpen() {
        return open.get();
    }

    @Override
    public void sendText(String text) {
        WebSocket s = ws;
        if(s == null) throw new IllegalStateException("WebSocket not connected yet");
        s.sendText(text, true);
    }

    @Override
    public void request(int n) {
        WebSocket s = ws;
        if(s == null || n <= 0) return;
        s.request(n);
    }
}
