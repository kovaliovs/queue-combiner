package com.revjet.interview.combiner;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author skovalyov
 */
public class CombinerImplTest {

    private CombinerImpl<Integer> combiner;

    @Test
    public void emptyInputQueueEviction() throws InterruptedException {
        combiner.addInput(5.0, 100, TimeUnit.MILLISECONDS);
        combiner.addInput(5.0, 100, TimeUnit.MILLISECONDS);
        assertEquals(2, combiner.numQueues());
        TimeUnit.SECONDS.sleep(1);
        assertEquals(0, combiner.numQueues());
    }

    @Test
    public void nonEmptyToEmptyInputQueueEviction() throws Exception {
        Combiner.CombinerInput<Integer> input1 = combiner.addInput(8.0, 100, TimeUnit.MILLISECONDS);
        Combiner.CombinerInput<Integer> input2 = combiner.addInput(2.0, 100, TimeUnit.MILLISECONDS);

        input1.put(10);
        input2.put(20);

        assertNotNull(combiner.poll());
        assertNotNull(combiner.poll());
        assertNull(combiner.poll());

        assertEquals(2, combiner.numQueues());

        TimeUnit.SECONDS.sleep(1);

        assertEquals(0, combiner.numQueues());
    }

    @Test
    public void combinerStochasticDistribution() throws Exception {
        Combiner.CombinerInput<Integer> input1 = combiner.addInput(8.0, 10, TimeUnit.SECONDS);
        Combiner.CombinerInput<Integer> input2 = combiner.addInput(2.0, 10, TimeUnit.SECONDS);

        for (int i = 0; i < 1000; i++) {
            input1.put(8);
            input2.put(2);
        }

        int countEighths = 0;
        int countSeconds = 0;
        for (int i = 0; i < 1000; i++) {
            switch (combiner.poll()) {
                case 8:
                    countEighths++;
                    break;
                case 2:
                    countSeconds++;
                    break;
            }
        }

        double expectedEighths = countEighths / 1000.0;
        double predictedEighths = 8.0 / 10.0;
        double diffEighths = Math.abs(expectedEighths - predictedEighths);

        assertTrue(diffEighths < 1e-1);

        double expectedSeconds = countSeconds / 1000.0;
        double predictedSeconds = 2.0 / 10.0;
        double diffSeconds = Math.abs(expectedSeconds - predictedSeconds);

        assertTrue(diffSeconds < 1e-1);
    }

    @Test
    public void pollWithTimeout() throws InterruptedException {
        Combiner.CombinerInput<Integer> input1 = combiner.addInput(5.0, 10000, TimeUnit.MILLISECONDS);
        Combiner.CombinerInput<Integer> input2 = combiner.addInput(5.0, 10000, TimeUnit.MILLISECONDS);
        assertEquals(2, combiner.numQueues());

        input1.put(10);
        input2.put(20);

        assertNotNull(combiner.poll(100, TimeUnit.MILLISECONDS));
        assertNotNull(combiner.poll(100, TimeUnit.MILLISECONDS));

        assertNull(combiner.poll(100, TimeUnit.MILLISECONDS));

        input1.put(10);
        assertNotNull(combiner.poll(100, TimeUnit.MILLISECONDS));
    }

    @Test(expected = IllegalStateException.class)
    public void putDataInRemovedCombinerInput() throws InterruptedException {
        Combiner.CombinerInput<Integer> input1 = combiner.addInput(5.0, 100, TimeUnit.MILLISECONDS);
        assertEquals(1, combiner.numQueues());

        assertNull(combiner.poll(100, TimeUnit.MILLISECONDS));

        input1.put(10);
    }

    @Before
    public void setup() {
        combiner = new CombinerImpl<>();
        combiner.start();
    }

    @After
    public void tearDown() {
        combiner.stop();
    }
}
