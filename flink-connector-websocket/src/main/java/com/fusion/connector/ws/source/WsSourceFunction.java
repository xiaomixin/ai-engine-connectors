package com.fusion.connector.ws.source;

import com.fusion.connector.ws.protocol.ProtocolAdapter;
import com.fusion.connector.ws.transport.WsClientEngine;
import org.apache.flink.streaming.api.functions.source.legacy.RichSourceFunction;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;

public class WsSourceFunction<C> extends RichSourceFunction<NormalizedEvent> {
    private final static Logger LOG = Logger.getLogger(WsSourceFunction.class.getName());

    private final C config;
    private final WsClientEngine engine;
    private final ProtocolAdapter<C> protocolAdapter;

    private final ArrayBlockingQueue<NormalizedEvent> queue;
    private final BoundedBackpressureController backpressure;

    private volatile boolean running;

    public WsSourceFunction(C config,
                            WsClientEngine engine,
                            ProtocolAdapter<C> protocolAdapter,
                            int queueCapacity,
                            int requestBatch) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.engine = Objects.requireNonNull(engine, "engine must not be null");
        this.protocolAdapter = Objects.requireNonNull(protocolAdapter, "protocolAdapter must not be null");
        if(queueCapacity <= 0){
            throw new IllegalArgumentException("queueCapacity must be greater than 0");
        }
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.backpressure = new BoundedBackpressureController(requestBatch);
    }

    @Override
    public void run(SourceContext<NormalizedEvent> ctx) throws Exception {
        this.running = true;
        engine.setListener(new WsClientEngine.WsMessageListener() {
            @Override
            public void onOpen() {
                for (String msg : protocolAdapter.initialMessages(config)) {
                    engine.sendText(msg);
                }
            }

            @Override
            public void onText(String message, boolean last) {
                if(!running) return;
                if(!last) return;
                if(protocolAdapter.isControlMessage(message)) return;

                backpressure.onWsMessageArrived();
                for (NormalizedEvent event : protocolAdapter.decode(config, message)) {
                    boolean ok = queue.offer(event);
                    if(!ok) break;
                }
                tryRequestMore();
            }

            @Override
            public void onError(Throwable error) {
                LOG.warning(error.getMessage());
            }

            @Override
            public void onClosed(int statusCode, String reason) {
                LOG.info("WebSocket closed: " + statusCode + " - " + reason);
            }
        });
        engine.connect();
    }

    private void tryRequestMore() {
        if(!running) return;
        if(!engine.isConnected()) return;

        int remainingCapacity = queue.remainingCapacity();
        int toRequest = backpressure.computeToRequest(remainingCapacity);
        if(toRequest > 0){
            engine.request(toRequest);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
        engine.close();
    }
}
