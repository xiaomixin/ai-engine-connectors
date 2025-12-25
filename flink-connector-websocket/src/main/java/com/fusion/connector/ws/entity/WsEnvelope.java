package com.fusion.connector.ws.entity;

import com.fasterxml.jackson.databind.JsonNode;

public record WsEnvelope(String channel, JsonNode data) {
}
