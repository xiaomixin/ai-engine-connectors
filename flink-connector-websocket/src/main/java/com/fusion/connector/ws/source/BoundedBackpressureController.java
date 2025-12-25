package com.fusion.connector.ws.source;

public class BoundedBackpressureController {

    private final int requestBatch;
    private int outstanding;

    public BoundedBackpressureController(int requestBatch) {
        if (requestBatch <= 0) {
            throw new IllegalArgumentException("requestBatch must be greater than 0");
        }
        this.requestBatch = requestBatch;
        this.outstanding = 0;
    }

    public void onWsMessageArrived() {
        if (outstanding > 0) {
            outstanding--;
        }
    }

    public int computeToRequest(int remainingCapacity) {
        if (remainingCapacity <= 0) {
            return 0;
        }
        if (outstanding >= requestBatch) {
            return 0;
        }

        int toRequest = Math.min(requestBatch - outstanding, remainingCapacity);
        outstanding += toRequest;
        return toRequest;
    }

    public int getOutstanding() {
        return outstanding;
    }

    public int getRequestBatch() {
        return requestBatch;
    }
}
