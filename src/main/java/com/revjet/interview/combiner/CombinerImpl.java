package com.revjet.interview.combiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * {@link Combiner} implementation.
 *
 * @author skovalyov
 */
public class CombinerImpl<T> implements Combiner<T> {
    private final Logger log = LoggerFactory.getLogger(CombinerImpl.class);

    private static final int REMOVE_QUEUE_DELAY = 100;

    private final List<CombinerInputQueue<T>> queues = new ArrayList<>();
    private Double maxWeight = 0.0;
    private final ReentrantLock mainLock = new ReentrantLock();

    private final ScheduledExecutorService removeQueuesService = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService pollService = Executors.newSingleThreadScheduledExecutor();

    @Override
    public CombinerInput<T> addInput(double priority, long isEmptyTimeout, TimeUnit timeUnit) {
        mainLock.lock();
        try {
            CombinerInputQueue<T> input = new CombinerInputQueue<>(priority, isEmptyTimeout, timeUnit);
            queues.add(input);
            if (priority > maxWeight) {
                maxWeight = priority;
            }
            return input;
        } finally {
            mainLock.unlock();
        }
    }

    @Override
    public T poll() {
        return pollNext();
    }

    @Override
    public T poll(long timeout, TimeUnit timeUnit) throws InterruptedException {
        T value = pollNext();
        if(value != null) {
            return value;
        }

        try {
            return pollService.schedule(this::pollNext, timeout, timeUnit).get();
        } catch (ExecutionException ignored) {
            return null;
        }
    }

    @Override
    public void start() {
        log.info("Start combiner");

        removeQueuesService.scheduleWithFixedDelay(() -> {
            while (!removeQueuesService.isShutdown()) {
                removeQueues();
            }
        }, REMOVE_QUEUE_DELAY, REMOVE_QUEUE_DELAY, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        removeQueuesService.shutdown();

        log.info("Stop combiner");
    }

    /**
     * Remove queues according to reached timeout
     * Update the max priority
     */
    private void removeQueues() {
        mainLock.lock();
        try {
            boolean shouldRecalculate = false;

            List<CombinerInputQueue<T>> toRemove = new ArrayList<>();
            for (CombinerInputQueue<T> queue : queues) {
                queue.recalculateTimeout();
                if(queue.isTimedOut()) {
                    toRemove.add(queue);
                    queue.remove();
                    if (queue.getPriority().equals(maxWeight)) {
                        shouldRecalculate = true;
                    }
                }
            }

            queues.removeAll(toRemove);

            if (shouldRecalculate) {
                maxWeight = calculateMaxWeight();
            }
        } finally {
            mainLock.unlock();
        }
    }

    /**
     * Calculate max weight
     * @return Max priority of the current set of queues.
     */
    private double calculateMaxWeight() {
        double max = 0.0;
        for (CombinerInputQueue<T> queue : queues) {
            if (queue.getPriority() > max) {
                max = queue.getPriority();
            }
        }
        return max;
    }

    /**
     * Use 'stochastic acceptance' to select a random weighted channel.
     * this code is based on.
     * https://en.wikipedia.org/wiki/Fitness_proportionate_selection#Java_-_stochastic_acceptance_version
     */
    private T pollNext() {
        mainLock.lock();
        try {
            List<CombinerInputQueue<T>> nonEmptyQueues = queues.stream()
                    .filter(inputQueue -> !inputQueue.isEmpty())
                    .collect(Collectors.toList());

            if(nonEmptyQueues.isEmpty()) {
                return null;
            }

            double mm = maxWeight;
            boolean notAccepted;
            int index = 0;
            Random rnd = new Random();
            notAccepted = true;
            while (notAccepted) {
                index = rnd.nextInt(nonEmptyQueues.size());
                double pp = nonEmptyQueues.get(index).getPriority();
                if (Math.random() < pp / mm) {
                    notAccepted = false;
                }
            }
            return nonEmptyQueues.get(index).poll();
        } finally {
            mainLock.unlock();
        }
    }

    int numQueues() {
        return queues.size();
    }

}
