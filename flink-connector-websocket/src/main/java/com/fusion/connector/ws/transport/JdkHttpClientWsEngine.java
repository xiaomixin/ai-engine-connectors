package com.fusion.connector.ws.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class JdkHttpClientWsEngine implements WsClientEngine {
    private static final Logger LOG = LoggerFactory.getLogger(JdkHttpClientWsEngine.class);
    private final URI wsUri;
    private final HttpClient httpClient;
    private final ExecutorService callbackExecutor;
    private final Duration connectTimeout;

    private volatile WsMessageListener listener;
    private volatile WebSocket webSocket;
    private final AtomicBoolean open = new AtomicBoolean(false);

    // For fragmented messages
    private final StringBuilder textBuffer = new StringBuilder(8 * 1024);
    private final Object textBufferLock = new Object();
    private final int maxBufferedChars;

    public JdkHttpClientWsEngine(String wsUrl, int connectTimeoutMs) {
        this(wsUrl, connectTimeoutMs, 4 * 1024 * 1024); // default 4MB buffer cap
    }

    public JdkHttpClientWsEngine(String wsUrl, int connectTimeoutMs, int maxBufferedChars) {
        this.wsUri = URI.create(Objects.requireNonNull(wsUrl, "wsUrl"));
        this.connectTimeout = Duration.ofMillis(Math.max(1000, connectTimeoutMs));
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(this.connectTimeout)
                .build();

        this.callbackExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "ws-callback-dispatcher");
            t.setDaemon(true);
            return t;
        });

        this.maxBufferedChars = Math.max(64 * 1024, maxBufferedChars);
    }

    @Override
    public void setListener(WsMessageListener listener) {
        this.listener = listener;
    }

    @Override
    public void close() {
        open.set(false);
        WebSocket ws = this.webSocket;
        if(ws != null){
            try {
                ws.sendClose(WebSocket.NORMAL_CLOSURE, "bye").join();
            } catch(Exception ignored) {
                LOG.warn("Error while closing WebSocket", ignored);
            }
        }
        callbackExecutor.shutdownNow();
        clearTextBuffer();
    }

    @Override
    public void connect() {
        this.httpClient.newWebSocketBuilder()
                .buildAsync(wsUri, new HttpWsListener())
                .whenComplete((ws, ex) -> {
                    if(ex != null){
                        dispatchError(ex);
                        dispatchClosed(1006, "connection failed");
                        return;
                    }
                    this.webSocket = ws;
                });
    }

    @Override
    public boolean isOpen() {
        return open.get();
    }

    @Override
    public void sendText(String text) {
        WebSocket s = this.webSocket;
        if(s == null) throw new IllegalStateException("WebSocket not connected yet");
        s.sendText(text, true);
    }

    @Override
    public void request(int n) {
        WebSocket ws = this.webSocket;
        if(ws == null) return;
        if(n <= 0) return;
        ws.request(n);
    }

    private void dispatchOpen() {
        WsMessageListener l = this.listener;
        if(l != null){
            callbackExecutor.execute(l::onOpen);
        }
    }

    private void dispatchText(String message, boolean last) {
        WsMessageListener l = this.listener;
        if(l != null){
            callbackExecutor.execute(() -> l.onText(message, last));
        }
    }

    private void dispatchError(Throwable error) {
        WsMessageListener l = this.listener;
        if(l != null){
            callbackExecutor.execute(() -> l.onError(error));
        }
    }

    private void dispatchClosed(int statusCode, String reason) {
        WsMessageListener l = this.listener;
        if(l != null){
            callbackExecutor.execute(() -> l.onClosed(statusCode, reason));
        }
    }

    private void appendFragment(CharSequence fragment) {
        synchronized (textBufferLock) {
            if(textBuffer.length() + fragment.length() > maxBufferedChars){
                LOG.warn("Text message buffer exceeded max size of {} chars, clearing buffer", maxBufferedChars);
                textBuffer.setLength(0);
                dispatchError(new IllegalStateException("Buffer exceeded max size of " + maxBufferedChars));
                return;
            }
            textBuffer.append(fragment);
        }
    }

    private String buildCompleteMessage(CharSequence lastPart) {
        synchronized (textBufferLock) {
            if(textBuffer.isEmpty()){
                return lastPart.toString();
            }
            if(textBuffer.length() + lastPart.length() > maxBufferedChars){
                textBuffer.setLength(0);
                dispatchError(new IllegalStateException("Buffer exceeded max size of " + maxBufferedChars));
                return null;
            }
            textBuffer.append(lastPart);
            String complete = textBuffer.toString();
            textBuffer.setLength(0);
            return complete;
        }
    }

    private void clearTextBuffer() {
        synchronized (textBufferLock) {
            textBuffer.setLength(0);
        }
    }


    private final class HttpWsListener implements WebSocket.Listener {
        @Override
        public void onOpen(WebSocket webSocket) {
            open.set(true);
            dispatchOpen();
            WebSocket.Listener.super.onOpen(webSocket);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            if(!open.get()){
                return WebSocket.Listener.super.onText(webSocket, data, last);
            }
            if(!last){
                appendFragment(data);
            } else {
                String complete = buildCompleteMessage(data);
                if(complete != null){
                    dispatchText(complete, true);
                }
            }
            return WebSocket.Listener.super.onText(webSocket, data, last);
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            dispatchError(error);
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            open.set(false);
            dispatchClosed(statusCode, reason);
            return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
        }
    }
}
