package com.fusion.connector.ws.transport;

import com.fusion.connector.ws.entity.WsTableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class FakeWsClientEngine implements WsClientEngine {
    private static final Logger LOG = LoggerFactory.getLogger(FakeWsClientEngine.class);
    private WsMessageListener listener;
    private final WsTableConfig wsTableConfig;

    private final AtomicBoolean open = new AtomicBoolean(false);
    private long seq = 0;

    public FakeWsClientEngine(WsTableConfig wsTableConfig) {
        this.wsTableConfig = wsTableConfig;
    }

    @Override
    public void connect() {
        LOG.info("FakeWsClientEngine connect");
        open.set(true);
        if(listener != null){
            listener.onOpen();
        }
    }

    @Override
    public void close() {
        open.set(false);
        if(listener != null) listener.onClosed(1000, "normal");
    }

    @Override
    public boolean isOpen() {
        return open.get();
    }

    @Override
    public void sendText(String message) {

    }

    @Override
    public void request(int n) {
        LOG.info("FakeWsClientEngine received request for {} messages", n);
        if(!isOpen() || listener == null) return;

        for (int i = 0; i < n; i++) {
            long now = System.currentTimeMillis();
            String payload = "{\"fake\":true,\"seq\":" + (seq++) +
                    ",\"channel\":\"" + wsTableConfig.channel() +
                    "\",\"symbol\":\"" + wsTableConfig.symbol() +
                    "\",\"ts\":" + now + "}";

            listener.onText(payload, true);
        }
    }

    @Override
    public void setListener(WsMessageListener listener) {
        this.listener = listener;
    }
}
