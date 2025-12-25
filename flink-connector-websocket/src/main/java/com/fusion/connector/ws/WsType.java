package com.fusion.connector.ws;

public enum WsType {
    HYPERLIQUID("hyperliquid"),
    BINANCE("binance");

    private final String typeName;

    WsType(String typeName) {
        this.typeName = typeName;
    }

    public String getTypeName() {
        return typeName;
    }
}
