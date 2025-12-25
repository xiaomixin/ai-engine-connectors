package com.fusion.connector.ws.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.table.data.RowData;

import java.util.List;

public interface ChannelRowMapper {
    String channel();

    List<RowData> map(JsonNode data);
}
