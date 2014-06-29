/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */
package si.pele.concurrent.test;

import si.pele.concurrent.ExpirableFutureTask;

import java.util.concurrent.*;

/**
 * @author peter.levart@gmail.com
 */
public class EFTTest {

    static ExpirableFutureTask<Integer> submitTasks(Executor executor, int n, long t0, long maxQueueSeconds, String taskNamePrefix, long taskSleepMillis) {
        println(t0, taskNamePrefix + " submission started");

        // submit root task with 'maxQueueSeconds' expiry...
        ExpirableFutureTask<Integer> task = ExpirableFutureTask.submit(
            () -> {
                println(t0, taskNamePrefix + "-root executing");
                java.lang.Thread.sleep(taskSleepMillis);
                return 1;
            },
            executor, maxQueueSeconds, TimeUnit.SECONDS
        );

        // ...connect to it a chain of n dependent tasks each depending on previous
        for (int i = 0; i < n; i++) {
            int ii = i;
            ExpirableFutureTask<Integer> prevTask = task;
            task = ExpirableFutureTask.submitAfter(
                () -> {
                    println(t0, taskNamePrefix + "-dependent" + ii + " executing");
                    Thread.sleep(taskSleepMillis);
                    return prevTask.get() + 1;
                },
                prevTask
            );
        }

        return task;
    }

    public static void main(String[] args) throws Exception {

        // fixed thread pool with single thread
        ExecutorService executor = new ThreadPoolExecutor(
            3, 3,
            30L, TimeUnit.SECONDS,
            new PriorityBlockingQueue<>() // prioritize tasks which expire earlier
        );

        long t0 = System.nanoTime();


        ExpirableFutureTask<Integer> task1 = submitTasks(executor, 10, t0, 10, "task1", 400L);
        Thread.sleep(100L);
        ExpirableFutureTask<Integer> task2 = submitTasks(executor, 10, t0, 11, "task2", 600L);
        Thread.sleep(100L);
        ExpirableFutureTask<Integer> task3 = submitTasks(executor, 10, t0, 12, "task3", 800L);
        Thread.sleep(100L);
        ExpirableFutureTask<Integer> task4 = submitTasks(executor, 10, t0, 13, "task4", 1000L);

        try {
            println(t0, "task1.get(): " + task1.get());
        } catch (ExecutionException e) {
            println(t0, "task1.get(): " + e);
        }

        try {
            println(t0, "task2.get(): " + task2.get());
        } catch (ExecutionException e) {
            println(t0, "task2.get(): " + e);
        }

        try {
            println(t0, "task3.get(): " + task3.get());
        } catch (ExecutionException e) {
            println(t0, "task3.get(): " + e);
        }

        try {
            println(t0, "task4.get(): " + task4.get());
        } catch (ExecutionException e) {
            println(t0, "task4.get(): " + e);
        }

        executor.shutdown();
    }

    static void println(long t0, String msg) {
        System.out.printf("%16s [t0 + %5d ms]: %s\n", Thread.currentThread().getName(), (System.nanoTime() - t0) / 1000000L, msg);
    }
}
