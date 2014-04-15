/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */

import si.pele.concurrent.queues.MPSCQueue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

/**
 * @author peter
 */
public class MPSCQueueTest {

    static class Consumer extends Thread {
        final BlockingQueue<?> queue;
        final int expectedCount;
        final CountDownLatch latch;
        long nanos;

        Consumer(BlockingQueue<?> queue, int expectedCount, CountDownLatch latch) {
            this.queue = queue;
            this.expectedCount = expectedCount;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                latch.await();
                long t0 = System.nanoTime();
                for (int i = 0; i < expectedCount; i++) {
                    queue.take();
                }
                nanos = System.nanoTime() - t0;
            } catch (InterruptedException e) {
                throw new Error(e);
            }
        }
    }

    static class Producer extends Thread {
        final BlockingQueue<Object> queue;
        final int count;
        final CountDownLatch latch;
        long nanos;

        Producer(BlockingQueue<Object> queue, int count, CountDownLatch latch) {
            this.queue = queue;
            this.count = count;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                latch.await();
                long t0 = System.nanoTime();
                for (int i = 0; i < count; i++) {
                    queue.put(Boolean.TRUE);
                }
                nanos = System.nanoTime() - t0;
            } catch (InterruptedException e) {
                throw new Error(e);
            }

        }
    }

    static double test(int elementCount, int producerCount, BlockingQueue<Object> queue) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        Consumer consumer = new Consumer(queue, producerCount * elementCount, latch);
        consumer.start();
        Producer[] producers = new Producer[producerCount];
        for (int i = 0; i < producerCount; i++) {
            (producers[i] = new Producer(queue, elementCount, latch)).start();
        }

        System.gc();

        latch.countDown();

        for (int i = 0; i < producerCount; i++) {
            producers[i].join();
        }
        consumer.join();

        System.gc();

        return ((double) producerCount * elementCount * 1000000000d / (double) consumer.nanos);
    }

    public static void main(String[] args) throws Exception {

        int _try = 0;
        for (int p = 1; p < 16; p++) {
            for (int t = 1; t <= 5; t++) {
                System.out.printf("try #%02d: %3d producers: %g messages/s\n", ++_try, p, test(16000000 / p, p, new MPSCQueue.Blocking<>()));
            }
            System.out.println();
        }

    }
}
