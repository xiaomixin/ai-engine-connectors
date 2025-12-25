package com.fusion.connector.ws.source;

import com.fusion.connector.ws.entity.WsTableConfig;
import org.apache.flink.legacy.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.types.DataType;

import java.util.Objects;

public class WsDynamicTableSource implements ScanTableSource {

    private final WsTableConfig cfg;
    private final DataType producedDataType;

    public WsDynamicTableSource(WsTableConfig cfg, DataType producedDataType) {
        this.cfg = Objects.requireNonNull(cfg);
        this.producedDataType = Objects.requireNonNull(producedDataType);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(
            ScanTableSource.ScanContext context) {
        WsRowDataSourceFunction sourceFunction = new WsRowDataSourceFunction(cfg, producedDataType);
        return SourceFunctionProvider.of(sourceFunction, false);
    }

    @Override
    public DynamicTableSource copy() {
        return new WsDynamicTableSource(cfg, producedDataType);

    }

    @Override
    public String asSummaryString() {
        return "WebSocketTableSource";
    }
}
