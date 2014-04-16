/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */

import si.pele.concurrent.queues.MPSCQueue;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

/**
 * @author peter
 */
public class MPSCQueueTest {

    static class Consumer extends Thread {
        final BlockingQueue<Integer> queue;
        final int consumeCount;
        final CountDownLatch latch;
        final int[] prducerHistogram;
        long nanos;

        Consumer(BlockingQueue<Integer> queue, int producers, int consumeCount, CountDownLatch latch) {
            this.queue = queue;
            this.consumeCount = consumeCount;
            this.latch = latch;
            this.prducerHistogram = new int[producers];
        }

        @Override
        public void run() {
            try {
                latch.await();

                long t0 = System.nanoTime();
                for (int i = 0; i < consumeCount; i++) {
                    int producerIndex = queue.take();
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
        final int produceCount;
        final CountDownLatch latch;
        long nanos;

        Producer(BlockingQueue<Integer> queue, int producerIndex, int produceCount, CountDownLatch latch) {
            this.queue = queue;
            this.producerIndex = producerIndex;
            this.produceCount = produceCount;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                Integer e = producerIndex;
                latch.await();

                long t0 = System.nanoTime();
                for (int i = 0; i < produceCount; i++) {
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
        int[] producerHistogram;
    }

    static Result test(int elementCount, int producers, Supplier<? extends BlockingQueue<Integer>> queueFactory) throws Exception {
        BlockingQueue<Integer> queue = queueFactory.get();
        CountDownLatch latch = new CountDownLatch(1);

        Consumer consumer = new Consumer(queue, producers, producers * elementCount, latch);
        consumer.start();
        Producer[] producerArray = new Producer[producers];
        for (int i = 0; i < producers; i++) {
            (producerArray[i] = new Producer(queue, i, elementCount, latch)).start();
        }

        System.gc();

        latch.countDown();

        Result r = new Result();
        r.producerNanos = new long[producers];

        for (int i = 0; i < producers; i++) {
            Producer producer = producerArray[i];
            producer.join();
            r.producerNanos[i] = producer.nanos;
        }
        consumer.join();
        r.consumerNanos = consumer.nanos;
        r.throughput = ((double) producers * elementCount * 1000000000d / (double) consumer.nanos);
        r.producerHistogram = consumer.prducerHistogram.clone();

        System.gc();

        return r;
    }

    static void doTest(String title, int tries, Supplier<? extends BlockingQueue<Integer>> queueFactory) throws Exception {
        System.out.printf("\n#\n# %s\n#\n", title);
        for (int p = 1; p < 16; p++) {
            double tpsum = 0d;
            for (int t = 1; t <= tries; t++) {
                Result r = test(16000000 / p, p, queueFactory);
                tpsum += r.throughput;
                System.out.printf("%3d producers, try #%02d: %g messages/s - %s\n", p, t, r.throughput, Arrays.toString(r.producerHistogram));
            }
            System.out.printf("%3d producers, average: %g messages/s\n\n", p, tpsum / tries);
        }
    }

    public static void main(String[] args) throws Exception {
        doTest("LinkedBlockingQueue(10000)", 5, () -> new LinkedBlockingQueue<Integer>(10000));
        doTest("MPSCQueue.Blocking(10000)", 5, () -> new MPSCQueue.Blocking<Integer>(10000));
    }
}
