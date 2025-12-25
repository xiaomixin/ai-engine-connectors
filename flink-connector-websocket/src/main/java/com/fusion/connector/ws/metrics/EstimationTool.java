package com.fusion.connector.ws.metrics;

import org.apache.flink.table.data.RowData;

public class EstimationTool {

    private EstimationTool() {
    }

    public static int estimateBytes(RowData rowData) {
        return estimateBytes(rowData.toString());
    }

    public static int estimateBytes(String message) {
        return message.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
    }
}
