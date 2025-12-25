package com.fusion.connector.ws.protocol;


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DecoderRegistry<C> {
    private final Map<String, ChannelDecoder<C>> decoders = new ConcurrentHashMap<>();

    public DecoderRegistry<C> register(ChannelDecoder<C> decoder) {
        decoders.put(decoder.channel(), decoder);
        return this;
    }

    public ChannelDecoder<C> get(String channel) {
        return decoders.get(channel);
    }
}
