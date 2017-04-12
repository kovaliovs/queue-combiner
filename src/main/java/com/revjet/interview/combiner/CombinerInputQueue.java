package com.revjet.interview.combiner;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A wrapper over class BlockingQueue to record metadata for that queue like empty time out
 *
 * @author skovalyov
 */
final class CombinerInputQueue<T> implements Combiner.CombinerInput<T> {
    private static final int QUEUE_SIZE = 1000;

    private final BlockingQueue<T> queue = new ArrayBlockingQueue<T>(QUEUE_SIZE);
    private final double priority;
    private AtomicBoolean isRemoved = new AtomicBoolean(false);

    private final Duration emptyTimeoutPeriod;
    private LocalDateTime emptyStartTime;
    private Duration emptyPeriod;

    CombinerInputQueue(double priority,
                       long emptyTimeoutPeriod,
                       TimeUnit timeUnit) {
        this.priority = priority;
        this.emptyTimeoutPeriod = Duration.ofNanos(TimeUnit.NANOSECONDS.convert(emptyTimeoutPeriod, timeUnit));

        recalculateTimeout();
    }

    @Override
    public void put(T value) {
        if(isRemoved.get()) {
            throw new IllegalStateException();
        }

        try {
            queue.put(value);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void remove() {
        isRemoved.set(true);
    }

    @Override
    public boolean isRemoved() {
        return isRemoved.get();
    }

    /**
     * Returns <tt>true</tt> if this collection contains no elements.
     *
     * @return <tt>true</tt> if this collection contains no elements
     */
    boolean isEmpty() {
        return queue.isEmpty();
    }

    /**
     * Retrieves and removes the head of this queue,
     * or returns {@code null} if this queue is empty.
     *
     * @return the head of this queue, or {@code null} if this queue is empty
     */
    T poll() {
        return queue.poll();
    }

    /**
     * Get input stream priority
     * @return the priority
     */
    Double getPriority() {
        return priority;
    }

    /**
     * Checks whether queue emptiness period is over.
     * @return  {@code true} if this queue has timed out.
     */
    boolean isTimedOut() {
        return emptyPeriod.compareTo(emptyTimeoutPeriod) > 0;
    }

    /**
     * Recalculate timeout if queue is empty
     * If there is no empty start time this will set one up.
     */
    void recalculateTimeout() {
        if(!queue.isEmpty()) {
            emptyPeriod = Duration.ZERO;
            emptyStartTime = null;
            return;
        }

        if(emptyStartTime == null) {
            emptyPeriod = Duration.ZERO;
            emptyStartTime = LocalDateTime.now();
        } else {
            Duration duration = Duration.between(emptyStartTime, LocalDateTime.now());
            emptyPeriod = emptyPeriod.plus(duration);
        }

    }

}
