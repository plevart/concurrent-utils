/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */

import si.pele.concurrent.queues.ConcurrentLinkedQueue_Yielding;
import si.pele.concurrent.queues.MPSCQueue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * @author peter
 */
public class MPSCQueueTest {

    static class Consumer extends Thread {
        final BlockingQueue<Integer> queue;
        final int consumeCount;
        final int[] prducerHistogram;
        long nanos;

        Consumer(BlockingQueue<Integer> queue, int producers, int consumeCount) {
            this.queue = queue;
            this.consumeCount = consumeCount;
            this.prducerHistogram = new int[producers];
        }

        @Override
        public void run() {
            try {
                int producerIndex = queue.take();
                prducerHistogram[producerIndex]++;

                long t0 = System.nanoTime();
                for (int i = 1; i < consumeCount; i++) {
                    producerIndex = queue.take();
                    prducerHistogram[producerIndex]++;
                }
                nanos = System.nanoTime() - t0;

            } catch (InterruptedException e) {
                throw new Error(e);
            }
        }
    }

    static class Producer extends Thread {
        final BlockingQueue<Integer> queue;
        final int producerIndex;
        final CountDownLatch startLatch;
        final AtomicBoolean stopSignal;
        long nanos;

        Producer(BlockingQueue<Integer> queue, int producerIndex, CountDownLatch startLatch, AtomicBoolean stopSignal) {
            this.queue = queue;
            this.producerIndex = producerIndex;
            this.startLatch = startLatch;
            this.stopSignal = stopSignal;
        }

        @Override
        public void run() {
            try {
                Integer e = producerIndex;
                startLatch.await();

                long t0 = System.nanoTime();
                while (!stopSignal.get()) {
                    queue.put(e);
                }
                nanos = System.nanoTime() - t0;

            } catch (InterruptedException e) {
                throw new Error(e);
            }
        }
    }

    static class Result {
        double throughput;
        long[] producerNanos;
        long consumerNanos;
        double[] producerHistogram;
    }

    static Result test(int producers, int consumeCount, BlockingQueue<Integer> queue) throws Exception {
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicBoolean stopSignal = new AtomicBoolean();

        Consumer consumer = new Consumer(queue, producers, consumeCount);
        consumer.start();
        Producer[] producerArray = new Producer[producers];
        for (int i = 0; i < producers; i++) {
            (producerArray[i] = new Producer(queue, i, startLatch, stopSignal)).start();
        }

        System.gc();

        Result r = new Result();
        r.producerNanos = new long[producers];
        r.producerHistogram = new double[producers];

        startLatch.countDown();

        consumer.join();
        stopSignal.set(true);

        for (int i = 0; i < 10 * producers; i++) {
            queue.poll();
        }
        for (int i = 0; i < producers; i++) {
            Producer producer = producerArray[i];
            producer.join();
            r.producerNanos[i] = producer.nanos;
            r.producerHistogram[i] = 100d * consumer.prducerHistogram[i] / consumeCount;
        }
        r.consumerNanos = consumer.nanos;
        r.throughput = ((double) consumeCount * 1000000000d / (double) consumer.nanos);

        System.gc();

        return r;
    }

    static void doTests(int maxProducers, int tries, int msgsPerTry, Supplier<? extends BlockingQueue<Integer>>... queueFactories) throws Exception {
        System.out.printf("\"# of producers\"");
        for (int i = 0; i < queueFactories.length; i++) {
            Class<?> qc = queueFactories[i].get().getClass();
            String title = qc.getCanonicalName().substring(qc.getPackage().getName().length() + 1);
            System.out.printf(", \"%s\"", title);
        }
        System.out.printf("\n");

        for (int p = 1; p <= maxProducers; p++) {
            System.out.printf("%d", p);
            for (int i = 0; i < queueFactories.length; i++) {
                double tpsum = 0d;
                for (int t = 0; t < tries; t++) {
                    Result r = test(p, msgsPerTry, queueFactories[i].get());
                    tpsum += r.throughput;
                }
                System.out.printf(", %g", tpsum / tries);
            }
            System.out.printf("\n");
        }
    }

    public static void main(String[] args) throws Exception {
        boolean spreadsheet = true;

        doTests(16, 5, 10000000,
            () -> new LinkedBlockingQueue<Integer>(10000),
            () -> new ArrayBlockingQueue<Integer>(10000),
            ConcurrentLinkedQueue_Yielding::new,
            LinkedTransferQueue::new,
            MPSCQueue.Yielding::new,
            () -> new MPSCQueue.Bounded.Yielding<Integer>(10000));
    }
}
