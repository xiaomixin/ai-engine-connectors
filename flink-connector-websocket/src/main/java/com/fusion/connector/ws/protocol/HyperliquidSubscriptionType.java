package com.fusion.connector.ws.protocol;

public enum HyperliquidSubscriptionType {
    HP_L2_BOOK("l2Book"),
    HP_TRADES("trades");

    private final String typeValue;

    HyperliquidSubscriptionType(String typeValue) {
        this.typeValue = typeValue;
    }

    public String getTypeValue() {
        return typeValue;
    }

    public static HyperliquidSubscriptionType fromChannel(String channel) {
        if (channel == null) {
            return null;
        }
        for (HyperliquidSubscriptionType type : values()) {
            if (type.typeValue.equals(channel)) {
                return type;
            }
        }
        return null;
    }
}

