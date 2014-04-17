package si.pele.concurrent.test;/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */

import si.pele.concurrent.queue.MPSCQueue;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * A test comparing throughput of {@link java.util.Queue}s using MPSC scenario
 *
 * @author peter.levart@gmail.com
 */
public class NonBlockingMPSCQueuesTest {

    static class Consumer extends Thread {
        final Queue<Integer> queue;
        final int consumeCount;
        final int[] prducerHistogram;
        long nanos;

        Consumer(Queue<Integer> queue, int producers, int consumeCount) {
            this.queue = queue;
            this.consumeCount = consumeCount;
            this.prducerHistogram = new int[producers];
        }

        @Override
        public void run() {
            for (int i = 0; i < 1000000; i++) {
                while (queue.poll() == null) {} ;
            }
            long t0 = System.nanoTime();
            for (int i = 0; i < consumeCount; i++) {
                Integer e;
                while ((e = queue.poll()) == null) {}
                prducerHistogram[e]++;
            }
            nanos = System.nanoTime() - t0;
        }
    }

    static class Producer extends Thread {
        final Queue<Integer> queue;
        final int producerIndex;
        final CountDownLatch startLatch;
        final AtomicBoolean stopSignal;
        long nanos;

        Producer(Queue<Integer> queue, int producerIndex, CountDownLatch startLatch, AtomicBoolean stopSignal) {
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
                    queue.offer(e);
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

    static Result test(int producers, int consumeCount, Queue<Integer> queue) throws Exception {
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

    static void doTests(int maxProducers, int tries, int msgsPerTry, Supplier<? extends Queue<Integer>>... queueFactories) throws Exception {
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

        int maxProducres = (args.length > 0)
                           ? Integer.parseInt(args[0])
                           : 16;

        doTests(maxProducres, 10, 5000000,
            ConcurrentLinkedQueue::new,
            MPSCQueue::new,
            () -> new MPSCQueue.Bounded(10000)
        );
    }
}
