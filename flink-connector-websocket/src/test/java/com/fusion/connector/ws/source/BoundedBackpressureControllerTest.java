package com.fusion.connector.ws.source;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("BoundedBackpressureController Tests")
class BoundedBackpressureControllerTest {

    private BoundedBackpressureController controller;

    @BeforeEach
    void setUp() {
        controller = new BoundedBackpressureController(10);
    }

    @Test
    @DisplayName("Should throw exception for invalid requestBatch")
    void testInvalidRequestBatch() {
        assertThrows(IllegalArgumentException.class, () -> new BoundedBackpressureController(0));
        assertThrows(IllegalArgumentException.class, () -> new BoundedBackpressureController(-1));
    }

    @Test
    @DisplayName("Should request full batch when no outstanding requests")
    void testRequestFullBatchWhenNoOutstanding() {
        int remainingCapacity = 100;
        int toRequest = controller.computeToRequest(remainingCapacity);
        
        assertEquals(10, toRequest, "Should request full batch size");
    }

    @Test
    @DisplayName("Should not request when remaining capacity is zero")
    void testNoRequestWhenNoCapacity() {
        int toRequest = controller.computeToRequest(0);
        assertEquals(0, toRequest, "Should not request when capacity is zero");
        
        toRequest = controller.computeToRequest(-1);
        assertEquals(0, toRequest, "Should not request when capacity is negative");
    }

    @Test
    @DisplayName("Should not request when outstanding equals batch size")
    void testNoRequestWhenOutstandingEqualsBatch() {
        int firstRequest = controller.computeToRequest(100);
        assertEquals(10, firstRequest, "First request should be full batch");
        
        int secondRequest = controller.computeToRequest(100);
        assertEquals(0, secondRequest, "Should not request when outstanding equals batch");
    }

    @Test
    @DisplayName("Should request partial batch when outstanding is less than batch")
    void testRequestPartialBatch() {
        controller.computeToRequest(100);
        controller.onWsMessageArrived();
        controller.onWsMessageArrived();
        
        int toRequest = controller.computeToRequest(100);
        assertEquals(2, toRequest, "Should request 2 more to reach batch size");
    }

    @Test
    @DisplayName("Should respect remaining capacity limit")
    void testRespectRemainingCapacity() {
        int remainingCapacity = 5;
        int toRequest = controller.computeToRequest(remainingCapacity);
        
        assertEquals(5, toRequest, "Should only request up to remaining capacity");
    }

    @Test
    @DisplayName("Should decrement outstanding when message arrives")
    void testDecrementOnMessageArrival() {
        int toRequest = controller.computeToRequest(100);
        assertEquals(10, toRequest, "Should have 10 outstanding");
        
        controller.onWsMessageArrived();
        toRequest = controller.computeToRequest(100);
        assertEquals(1, toRequest, "Should request 1 more after one message arrived");
    }

    @Test
    @DisplayName("Should handle multiple message arrivals")
    void testMultipleMessageArrivals() {
        controller.computeToRequest(100);
        
        for (int i = 0; i < 5; i++) {
            controller.onWsMessageArrived();
        }
        
        int toRequest = controller.computeToRequest(100);
        assertEquals(5, toRequest, "Should request 5 more after 5 messages arrived");
    }

    @Test
    @DisplayName("Should not go negative on excessive message arrivals")
    void testNoNegativeOutstanding() {
        controller.computeToRequest(100);
        
        for (int i = 0; i < 15; i++) {
            controller.onWsMessageArrived();
        }
        
        int toRequest = controller.computeToRequest(100);
        assertEquals(10, toRequest, "Should request full batch again");
    }

    @Test
    @DisplayName("Should handle complex backpressure scenario")
    void testComplexBackpressureScenario() {
        int batchSize = 10;
        controller = new BoundedBackpressureController(batchSize);
        
        int remainingCapacity = 50;
        
        int toRequest1 = controller.computeToRequest(remainingCapacity);
        assertEquals(batchSize, toRequest1, "Initial request should be full batch");
        assertEquals(batchSize, controller.getOutstanding(), "Outstanding should equal batch size");
        
        remainingCapacity -= toRequest1;
        
        controller.onWsMessageArrived();
        controller.onWsMessageArrived();
        assertEquals(batchSize - 2, controller.getOutstanding(), "Outstanding should decrease by 2");
        
        int toRequest2 = controller.computeToRequest(remainingCapacity);
        assertEquals(2, toRequest2, "Should request 2 more after 2 messages processed");
        assertEquals(batchSize, controller.getOutstanding(), "Outstanding should be back to batch size");
        
        remainingCapacity -= toRequest2;
        
        for (int i = 0; i < 8; i++) {
            controller.onWsMessageArrived();
        }
        assertEquals(2, controller.getOutstanding(), "Outstanding should be 2 after 8 messages");
        
        int toRequest3 = controller.computeToRequest(remainingCapacity);
        assertEquals(8, toRequest3, "Should request 8 more to reach batch size");
        assertEquals(batchSize, controller.getOutstanding(), "Outstanding should be back to batch size");
        
        controller.onWsMessageArrived();
        controller.onWsMessageArrived();
        assertEquals(batchSize - 2, controller.getOutstanding(), "Outstanding should decrease by 2");
        
        int toRequest4 = controller.computeToRequest(remainingCapacity);
        assertEquals(2, toRequest4, "Should request 2 more after 2 more messages processed");
    }

    @Test
    @DisplayName("Should handle edge case with small remaining capacity")
    void testSmallRemainingCapacity() {
        controller.computeToRequest(100);
        controller.onWsMessageArrived();
        controller.onWsMessageArrived();
        
        int toRequest = controller.computeToRequest(1);
        assertEquals(1, toRequest, "Should only request what capacity allows");
        
        toRequest = controller.computeToRequest(1);
        assertEquals(1, toRequest, "Should request 1 more if capacity allows");
    }

    @Test
    @DisplayName("Should handle rapid request and arrival cycles")
    void testRapidCycles() {
        for (int cycle = 0; cycle < 10; cycle++) {
            int toRequest = controller.computeToRequest(100);
            assertTrue(toRequest > 0, "Should request in each cycle");
            
            for (int i = 0; i < toRequest; i++) {
                controller.onWsMessageArrived();
            }
        }
        
        int finalRequest = controller.computeToRequest(100);
        assertEquals(10, finalRequest, "Should be able to request full batch after cycles");
    }

    @Test
    @DisplayName("Should correctly track outstanding count")
    void testOutstandingTracking() {
        assertEquals(0, controller.getOutstanding(), "Initial outstanding should be 0");
        assertEquals(10, controller.getRequestBatch(), "Request batch should be 10");
        
        controller.computeToRequest(100);
        assertEquals(10, controller.getOutstanding(), "Outstanding should be 10 after requesting");
        
        controller.onWsMessageArrived();
        assertEquals(9, controller.getOutstanding(), "Outstanding should be 9 after one message");
        
        controller.onWsMessageArrived();
        controller.onWsMessageArrived();
        assertEquals(7, controller.getOutstanding(), "Outstanding should be 7 after three messages");
        
        controller.computeToRequest(100);
        assertEquals(10, controller.getOutstanding(), "Outstanding should be back to 10");
    }

    @Test
    @DisplayName("Should handle exact capacity match")
    void testExactCapacityMatch() {
        controller.computeToRequest(10);
        assertEquals(10, controller.getOutstanding(), "Should have 10 outstanding");
        
        for (int i = 0; i < 10; i++) {
            controller.onWsMessageArrived();
        }
        assertEquals(0, controller.getOutstanding(), "Should have 0 outstanding after all messages");
        
        int toRequest = controller.computeToRequest(10);
        assertEquals(10, toRequest, "Should request full batch again");
        assertEquals(10, controller.getOutstanding(), "Should have 10 outstanding again");
    }
}

