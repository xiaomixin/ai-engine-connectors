package com.fusion.connector.ws.transport;

public interface WsClientEngine {

    void connect();
    void close();
    boolean isOpen();

    void sendText(String message);
    void request(int n);
    void setListener(WsMessageListener listener);

    interface WsMessageListener {
        void onOpen();
        void onText(String message, boolean last);
        void onError(Throwable error);
        void onClosed(int statusCode, String reason);
    }
}
