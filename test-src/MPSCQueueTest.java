/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */

import si.pele.concurrent.queues.MPSCQueue;

import java.util.Queue;
import java.util.concurrent.CountDownLatch;

/**
 * @author peter
 */
public class MPSCQueueTest {

    static class Consumer extends Thread {
        final Queue<?> queue;
        final int expectedCount;
        final CountDownLatch latch;
        long nanos;

        Consumer(Queue<?> queue, int expectedCount, CountDownLatch latch) {
            this.queue = queue;
            this.expectedCount = expectedCount;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new Error(e);
            }
            long t0 = System.nanoTime();
            for (int i = 0; i < expectedCount; i++) {
                while (queue.poll() == null) {
                    Thread.yield();
                }
            }
            nanos = System.nanoTime() - t0;
        }
    }

    static class Producer extends Thread {
        final Queue<Object> queue;
        final int count;
        final CountDownLatch latch;
        long nanos;

        Producer(Queue<Object> queue, int count, CountDownLatch latch) {
            this.queue = queue;
            this.count = count;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new Error(e);
            }
            long t0 = System.nanoTime();
            for (int i = 0; i < count; i++) {
                queue.add(Boolean.TRUE);
            }
            nanos = System.nanoTime() - t0;
        }
    }

    static void test(int _try, int elementCount, int producerCount) throws Exception {
        Queue<Object> queue = new MPSCQueue<>();
        CountDownLatch latch = new CountDownLatch(1);

        Consumer consumer = new Consumer(queue, producerCount * elementCount, latch);
        consumer.start();
        Producer[] producers = new Producer[producerCount];
        for (int i = 0; i < producerCount; i++) {
            (producers[i] = new Producer(queue, elementCount, latch)).start();
        }

        System.out.println("try #" + _try + ": " + producerCount + " producers with single consumer, " + (producerCount * elementCount) + " messages in total...");
        System.gc();

        latch.countDown();

        for (int i = 0; i < producerCount; i++) {
            producers[i].join();
            System.out.println("        Producer #" + (i + 1) + ": " + producers[i].nanos + " ns");
        }

        consumer.join();
        System.out.println("        Consumer   : " + consumer.nanos + " ns (throughput: " + ((double) producerCount * elementCount * 1000000000d / (double) consumer.nanos) + " messages/s)");

        System.gc();
    }

    public static void main(String[] args) throws Exception {

        int _try = 0;
        for (int p = 7; p < 16; p++)
            for (int t = 1; t <= 5; t++)
                test(++_try, 10000000, p);

    }
}
