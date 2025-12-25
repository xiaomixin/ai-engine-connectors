package com.fusion.connector.ws.table;

import com.fusion.connector.ws.entity.WsTableConfig;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;

public class WsRowDataSourceFunction extends RichParallelSourceFunction<RowData> {

    private final WsTableConfig cfg;
    private final DataType producedDataType;

    private volatile boolean running = true;


    public WsRowDataSourceFunction(WsTableConfig cfg, DataType producedDataType) {
        this.cfg = cfg;
        this.producedDataType = producedDataType;
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        while (running) {
            long now = System.currentTimeMillis();
            String payload = "{\"mock\":true,\"channel\":\"" + cfg.channel() + "\",\"symbol\":\"" + cfg.symbol() + "\",\"ts\":" + now + "}";

            GenericRowData row = new GenericRowData(5);
            row.setField(0, StringData.fromString(cfg.source()));
            row.setField(1, StringData.fromString(cfg.channel()));
            row.setField(2, StringData.fromString(cfg.symbol()));
            row.setField(3, now); // BIGINT
            row.setField(4, StringData.fromString(payload));

            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(row);
            }

            Thread.sleep(200);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
