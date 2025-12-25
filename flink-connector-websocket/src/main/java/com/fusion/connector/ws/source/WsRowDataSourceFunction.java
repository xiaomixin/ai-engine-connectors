package com.fusion.connector.ws.source;

import com.fusion.connector.ws.entity.UnknownChannelPolicy;
import com.fusion.connector.ws.entity.WsTableConfig;
import com.fusion.connector.ws.protocol.*;
import com.fusion.connector.ws.transport.HttpWsClientEngine;
import com.fusion.connector.ws.transport.WsClientEngine;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.source.legacy.RichSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class WsRowDataSourceFunction extends RichSourceFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(WsRowDataSourceFunction.class);
    private final WsTableConfig cfg;
    private final DataType producedDataType;

    private volatile boolean running = true;

    private transient WsClientEngine engine;
    private transient ProtocolAdapter<WsTableConfig> adapter;

    private transient ArrayBlockingQueue<NormalizedEvent> queue;
    private transient BoundedBackpressureController backpressure;


    public WsRowDataSourceFunction(WsTableConfig cfg, DataType producedDataType) {
        this.cfg = cfg;
        this.producedDataType = producedDataType;
    }

    @Override
    public void open(OpenContext context) throws Exception {
        this.queue = new ArrayBlockingQueue<>(cfg.queueCapacity());
        this.backpressure = new BoundedBackpressureController(cfg.requestBatch());

        DecoderRegistry<WsTableConfig> reg = new DecoderRegistry<>();
        reg.register(new L2BookDecoder());
        this.adapter = new HyperliquidWsAdapter(reg, UnknownChannelPolicy.DROP);
        this.engine = new HttpWsClientEngine(cfg.url(), 10_000);
        this.engine.setListener(new WsClientEngine.WsMessageListener() {
            @Override
            public void onOpen() {
                for (String msg : adapter.initialMessages(cfg)) {
                    engine.sendText(msg);
                }
                tryRequestMore();
            }

            @Override
            public void onText(String message, boolean last) {
                if(!running || !last) return;
                if(adapter.isControlMessage(message)){
                    LOG.info("Received control message: {}", message);
                    return;
                }
                backpressure.onWsMessageArrived();
                for (NormalizedEvent event : adapter.decode(cfg, message)) {
                    if(!queue.offer(event)) break;
                }
                tryRequestMore();
            }

            @Override
            public void onError(Throwable error) {
                LOG.error(error.getMessage(), error);
            }

            @Override
            public void onClosed(int statusCode, String reason) {
                LOG.info("Websocket connection closed: {}", reason);
            }
        });

        this.engine.connect();
    }

    private void tryRequestMore() {
        if(!running || engine == null || !engine.isOpen()) return;
        int remainingCapacity = queue.remainingCapacity();
        LOG.info("Remaining Capacity: {}", remainingCapacity);
        int toRequest = backpressure.computeToRequest(remainingCapacity);
        LOG.info("To Request: {}", toRequest);
        if(toRequest > 0){
            engine.request(toRequest);
        }
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        while (running) {
            NormalizedEvent event = queue.poll(200, TimeUnit.MICROSECONDS);
            if(event == null){
                tryRequestMore();
                continue;
            }
            RowData raw = toRowData(event);
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(raw);
            }
            tryRequestMore();
        }
    }

    private RowData toRowData(NormalizedEvent event) {
        GenericRowData row = new GenericRowData(5);
        row.setField(0, StringData.fromString(event.source()));
        row.setField(1, StringData.fromString(event.channel()));
        row.setField(2, StringData.fromString(event.symbol()));
        row.setField(3, event.eventTimeMs());
        row.setField(4, StringData.fromString(event.payloadJson()));
        return row;
    }

    @Override
    public void cancel() {
        running = false;
    }
}
