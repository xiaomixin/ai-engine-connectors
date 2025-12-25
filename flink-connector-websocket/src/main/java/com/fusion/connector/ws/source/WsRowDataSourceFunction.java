package com.fusion.connector.ws.source;

import com.fusion.connector.ws.entity.WsEnvelope;
import com.fusion.connector.ws.entity.WsTableConfig;
import com.fusion.connector.ws.mapping.ChannelRowMapper;
import com.fusion.connector.ws.mapping.RowMapperFactory;
import com.fusion.connector.ws.protocol.*;
import com.fusion.connector.ws.transport.HttpWsClientEngine;
import com.fusion.connector.ws.transport.WsClientEngine;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.source.legacy.RichSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class WsRowDataSourceFunction extends RichSourceFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(WsRowDataSourceFunction.class);
    private final WsTableConfig cfg;
    private final DataType producedDataType;

    private volatile boolean running = true;

    private transient WsClientEngine engine;
    private transient ProtocolAdapter<WsTableConfig> adapter;
    private transient ChannelRowMapper rowMapper;

    private transient ArrayBlockingQueue<RowData> queue;
    private transient BoundedBackpressureController backpressure;


    public WsRowDataSourceFunction(WsTableConfig cfg, DataType producedDataType) {
        this.cfg = cfg;
        this.producedDataType = producedDataType;
    }

    @Override
    public void open(OpenContext context) throws Exception {
        this.queue = new ArrayBlockingQueue<>(cfg.queueCapacity());
        this.backpressure = new BoundedBackpressureController(cfg.requestBatch());

        RowType rowType = (RowType) producedDataType.getLogicalType();
        this.rowMapper = RowMapperFactory.create(cfg.channel(), rowType);

        this.adapter = new HyperliquidWsAdapter();
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
                WsEnvelope envelope = adapter.parseEnvelope(message);
                if(envelope == null || envelope.data() == null){
                    LOG.warn("Failed to parse message or empty data: {}", message);
                    return;
                }
                List<RowData> mappedRows = rowMapper.map(envelope.data());
                for (RowData row : mappedRows) {
                    if(!queue.offer(row)) break;
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
        int toRequest = backpressure.computeToRequest(remainingCapacity);
        if(toRequest > 0){
            engine.request(toRequest);
        }
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        while (running) {
            RowData rowData = queue.poll(200, TimeUnit.MICROSECONDS);
            if(rowData == null){
                tryRequestMore();
                continue;
            }
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(rowData);
            }
            tryRequestMore();
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
