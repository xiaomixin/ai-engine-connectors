package com.fusion.connector.ws.table;

import com.fusion.connector.ws.entity.WsTableConfig;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static com.fusion.connector.ws.table.WsConnectorOptions.*;

public class WsDynamicTableSourceFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "ws";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(WS_URL);
        options.add(WS_SOURCE);
        options.add(WS_CHANNEL);
        options.add(WS_SYMBOL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of(WS_SOURCE,
                WS_QUEUE_CAPACITY,
                WS_REQUEST_BATCH,
                WS_CONNECT_TIMEOUT_MS,
                WS_RECONNECT_MAX_RETRIES,
                WS_RECONNECT_BACKOFF_MS
        );
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        final ReadableConfig options = helper.getOptions();

        final String url = options.get(WS_URL);
        final String source = options.get(WS_SOURCE);
        final String channel = options.get(WS_CHANNEL);
        final String symbol = options.get(WS_SYMBOL);
        final int queueCapacity = options.get(WS_QUEUE_CAPACITY);
        final int requestBatch = options.get(WS_REQUEST_BATCH);

        if(queueCapacity <= 0){
            throw new IllegalArgumentException("ws.queue.capacity must be greater than 0");
        }
        if(requestBatch <= 0){
            throw new IllegalArgumentException("ws.request.batch must be greater than 0");
        }
        if(requestBatch > queueCapacity){
            throw new IllegalArgumentException("ws.request.batch must not be greater than ws.queue.capacity");
        }
        final var producedDataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();
        final WsTableConfig cfg = new WsTableConfig(url, source, channel, symbol, queueCapacity, requestBatch);

        return new WsDynamicTableSource(cfg, producedDataType);
    }
}
