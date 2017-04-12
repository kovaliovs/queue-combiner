package com.revjet.interview.combiner;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author skovalyov
 */
public class CombinerInputQueueTest {

    @Test
    public void isTimedEmptyQueue() throws Exception {
        CombinerInputQueue inputQueue = new CombinerInputQueue<Integer>(9.0, 1, TimeUnit.MICROSECONDS);
        TimeUnit.SECONDS.sleep(1);
        inputQueue.recalculateTimeout();
        assertTrue(inputQueue.isTimedOut());
    }

    @Test
    public void isTimedOutTempDataInQueue() throws Exception {
        CombinerInputQueue inputQueue = new CombinerInputQueue<Integer>(9.0, 1, TimeUnit.MICROSECONDS);
        inputQueue.put(1);
        inputQueue.recalculateTimeout();
        assertEquals(1, inputQueue.poll());
        inputQueue.recalculateTimeout();
        TimeUnit.SECONDS.sleep(1);
        inputQueue.recalculateTimeout();
        assertTrue(inputQueue.isTimedOut());
    }

    @Test
    public void isTimedOutNonEmptyQueue() throws Exception {
        CombinerInputQueue inputQueue = new CombinerInputQueue<Integer>(9.0, 1, TimeUnit.MICROSECONDS);
        inputQueue.put(1);
        inputQueue.recalculateTimeout();
        TimeUnit.SECONDS.sleep(1);
        inputQueue.recalculateTimeout();
        assertFalse(inputQueue.isTimedOut());
    }

    @Test
    public void isRemoved() {
        CombinerInputQueue inputQueue = new CombinerInputQueue<Integer>(9.0, 1, TimeUnit.MICROSECONDS);
        assertFalse(inputQueue.isRemoved());
        inputQueue.remove();
        assertTrue(inputQueue.isRemoved());
    }

    @Test(expected = IllegalStateException.class)
    public void putDataInRemovedCombinerInput() throws InterruptedException {
        CombinerInputQueue inputQueue = new CombinerInputQueue<Integer>(9.0, 1, TimeUnit.MICROSECONDS);
        assertFalse(inputQueue.isRemoved());
        inputQueue.remove();
        inputQueue.put(1);
    }
}
