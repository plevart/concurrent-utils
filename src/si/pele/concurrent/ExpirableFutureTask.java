/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */
package si.pele.concurrent;

import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link java.util.concurrent.FutureTask} which is also {@link ExpirableFuture}.<p>
 * Instances are obtained by factory methods: {@link #submit} and {@link #submitAfter}.
 *
 * @author peter.levart@gmail.com
 */
public class ExpirableFutureTask<V>
    extends FutureTask<V>
    implements ExpirableFuture<V> {

    // deadline in terms of System.nanoTime()
    private final long deadline;

    // executor used to run this and dependent tasks
    private final Executor executor;

    // number of dependencies
    private final AtomicInteger uncompletedDependencies;

    private ExpirableFutureTask(Callable<V> callable, long deadline, Executor executor, int dependencies) {
        super(callable);
        this.deadline = deadline;
        this.executor = executor;
        this.uncompletedDependencies = new AtomicInteger(dependencies);
    }

    // dependent tasks
    private final Queue<ExpirableFutureTask<?>> dependents = new ConcurrentLinkedQueue<>();

    @Override
    public void run() {
        if (!isDone()) { // can already be done if expired by expired dependency
            if (deadline > System.nanoTime()) {
                super.run();
            } else {
                expire();
            }
        }
    }

    @Override
    public long deadline() {
        return deadline;
    }

    @Override
    protected void done() {
        processDependentsIfDone();
    }

    private void expire() {
        setException(new ExpiredException());
    }

    private void dependencyDone() {
        // last dependency done?
        if (uncompletedDependencies.decrementAndGet() <= 0) {
            // submit for execution if not done (expired) already
            if (!isDone()) {
                executor.execute(this);
            }
        }
    }

    private void processDependentsIfDone() {
        if (isDone()) {
            ExpirableFutureTask<?> task;
            if (isExpired()) {
                // expiry is viral -> expire all dependents
                while ((task = dependents.poll()) != null) {
                    task.expire();
                }
            } else {
                // notify all dependents that one dependency is done
                while ((task = dependents.poll()) != null) {
                    task.dependencyDone();
                }
            }
        }
    }

    public boolean isExpired() {
        if (isDone()) {
            try {
                get();
            } catch (InterruptedException ignore) {
                // can't happen since we are done
            } catch (ExecutionException e) {
                return e.getCause() instanceof ExpiredException;
            }
        }
        return false;
    }

    // factory methods

    /**
     * Submits a {@code callable} for execution to the given {@code executor} and returns
     * an {@link ExpirableFutureTask} which expires if it waits in the executor's queue
     * for more than {@code maxQueueTime [timeUnit]}.
     *
     * @param callable     the {@link Callable} to submit for execution
     * @param executor     the {@link Executor} to use (preferably using a
     *                     {@link java.util.concurrent.PriorityBlockingQueue} to prioritize tasks which expire sooner)
     * @param maxQueueTime maximum time the submitted task can wait in queue before it expires
     * @param timeUnit     the {@link java.util.concurrent.TimeUnit} used for {@code maxQueueTime}
     * @return an {@link ExpirableFutureTask} representing an {@link ExpirableFuture}
     *         of the computation represented by given {@code callable}
     * @see #submitAfter
     */
    public static <V> ExpirableFutureTask<V> submit(
        Callable<V> callable,
        Executor executor,
        long maxQueueTime, TimeUnit timeUnit
    ) {

        ExpirableFutureTask<V> task = new ExpirableFutureTask<>(
            callable,
            System.nanoTime() + timeUnit.toNanos(maxQueueTime),
            executor,
            0
        );

        executor.execute(task);

        return task;
    }

    /**
     * Arranges for given {@code callable} to be submitted to the {@link Executor}
     * associated with the dependency which expires earliest.<p>
     * It will be submitted after all of the {@code dependencies} are done or at
     * least one of them expires in which case this task will expire too.
     * The {@link #deadline()} of the returned {@link ExpirableFutureTask} is set
     * to the deadline of the dependency which expires earliest.
     *
     * @param callable     the callable to be submitted for execution
     * @param dependencies the dependencies that must complete first before
     *                     given callable is submitted for execution
     * @return an {@link ExpirableFutureTask} representing an {@link ExpirableFuture}
     *         of the computation represented by given {@code callable}
     * @see #submit
     */
    public static <V> ExpirableFutureTask<V> submitAfter(
        Callable<V> callable,
        ExpirableFutureTask<?>... dependencies
    ) {

        if (dependencies == null || dependencies.length < 1) {
            throw new IllegalArgumentException("There must be at least one dependency");
        }

        // deadline and executor is taken from the dependency which expires earliest
        Executor executor = dependencies[0].executor;
        long deadline = dependencies[0].deadline;
        for (int i = 1; i < dependencies.length; i++) {
            if (dependencies[i].deadline < deadline) {
                executor = dependencies[i].executor;
                deadline = dependencies[i].deadline;
            }
        }

        ExpirableFutureTask<V> task = new ExpirableFutureTask<>(
            callable,
            deadline,
            executor,
            dependencies.length
        );

        for (ExpirableFutureTask<?> dependency : dependencies) {
            dependency.dependents.add(task);
            dependency.processDependentsIfDone();
        }

        return task;
    }
}
