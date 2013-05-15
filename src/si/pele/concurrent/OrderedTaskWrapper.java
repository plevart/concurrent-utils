/*
 * Copyright (C) 2013 Peter Levart
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package si.pele.concurrent;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A wrapper for {@link Runnable} objects that {@link #wrap wraps} each given {@code Runnable} with a delegating
 * implementation that arranges concurrent and un-ordered executions of each individual wrapper in such a way
 * that executions of wrapped {@code Runnable}s associated within each distinct key happen serially in order
 * of wrapping them.<p>
 * The only rule to follow is that each wrapper must be invoked exactly once. This can be accomplished by passing
 * wrappers to an {@link java.util.concurrent.Executor} for execution.<p>
 *
 * @author peter.levart@gmail.com
 */
public class OrderedTaskWrapper<K> {

    /**
     * Wrap given {@code task} and associated {@code key} and return a wrapper
     * {@link Runnable} which must be called exactly once. The associated key identifies
     * the "execution group" within which the wrapped tasks are executed serially and in
     * order of wrapping them.<p>
     * Each wrapped task is executed either when it's wrapper is executed or when the preceding
     * task within the same "execution group" has finished executing - whichever happens last.<p>
     *
     * @param task the {@code Runnable} to wrap
     * @param key  the {@code key} to associate with the {@code task} which identifies the
     *             execution group.
     * @return wrapper {@code Runnable} which must be executed exactly once if the wrapped task or any
     *         subsequently wrapped tasks for the same execution group are to be executed.
     */
    public Runnable wrap(Runnable task, K key) {
        OrderedTask newTask = new OrderedTask(task, key);
        OrderedTask lastTask = lastTasksMap.put(key, newTask);
        if (lastTask != null) {
            // last task not finished yet -> link new task to the chain
            // this should never block since at most one element is enqueue-ed in each
            // {@link OrderedTask#next} queue
            newTask.state.set(State.CHAINED);
            putUninterruptibly(lastTask.next, newTask);
        }
        else {
            // last task already finished -> new task is now first in (new) chain
            newTask.state.set(State.FIRST);
        }
        return newTask;
    }

    // a map from key to the last task wrapped - cleaned-up automatically when last task has finished
    // before any new task is wrapped for the same key
    private final ConcurrentMap<K, OrderedTask> lastTasksMap = new ConcurrentHashMap<>();

    private enum State {
        FIRST,     // first in chain
        CHAINED,   // not-first in chain
        TRIGGERED  // executor TRIGGERED it by invoking run()
    }

    private final class OrderedTask implements Runnable {
        private final Runnable task;
        private final K key;
        final AtomicReference<State> state = new AtomicReference<>();
        final BlockingQueue<OrderedTask> next = new ArrayBlockingQueue<>(1);
        private Throwable throwable;

        OrderedTask(Runnable task, K key) {
            this.task = task;
            this.key = key;
        }

        @Override
        public void run() {
            // external invocation of run() TRIGGERS 'this' OrderedTask
            if (State.FIRST == state.getAndSet(State.TRIGGERED)) {
                // only proceed with execution if 'this' was FIRST in chain
                for (OrderedTask ordered = this; ordered != null; ) {
                    try {
                        ordered.task.run();
                    }
                    catch (Throwable t) {
                        if (throwable == null) {
                            throwable = t;
                        }
                        else {
                            try {
                                throwable.addSuppressed(t);
                            }
                            catch (Throwable t2) { }
                        }
                    }
                    finally {
                        if (lastTasksMap.remove(key, ordered)) {
                            // 'ordered' was last in chain -> terminate loop
                            ordered = null;
                        }
                        else {
                            // 'ordered' was not last in chain -> take next
                            ordered = takeUninterruptibly(ordered.next);
                            // make next 'ordered' FIRST in chain to allow possible
                            // external invocation of ordered.run() to execute it...
                            if (State.TRIGGERED != ordered.state.getAndSet(State.FIRST)) {
                                // only proceed with execution in this loop if 'ordered' was already TRIGGERED
                                // else terminate loop and let future external invocation of ordered.run() execute it
                                ordered = null;
                            }
                        }
                    }
                }
                // throw any accumulated exception(s)
                if (throwable != null) {
                    if (throwable instanceof RuntimeException) {
                        throw (RuntimeException) throwable;
                    }
                    else if (throwable instanceof Error) {
                        throw (Error) throwable;
                    }
                    else {
                        throw new UndeclaredThrowableException(throwable);
                    }
                }
            }
        }
    }

    /**
     * {@link BlockingQueue#put} ignoring interrupts.
     */
    static <T> void putUninterruptibly(BlockingQueue<T> queue, T element) {
        boolean interrupted = false;
        try {
            for (; ; ) {
                try {
                    queue.put(element);
                    return;
                }
                catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        finally {
            // restore the interrupted status if InterruptedException has been thrown
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * {@link BlockingQueue#take} ignoring interrupts.
     */
    static <T> T takeUninterruptibly(BlockingQueue<T> queue) {
        boolean interrupted = false;
        try {
            for (; ; ) {
                try {
                    return queue.take();
                }
                catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        finally {
            // restore the interrupted status if InterruptedException has been thrown
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }


    // test
    public static void main(String[] args) {

        class Task implements Runnable {
            final int key, seq;

            Task(int key, int seq) {
                this.key = key;
                this.seq = seq;
            }

            @Override
            public void run() {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < key; i++) sb.append("        ");
                sb.append(seq).append(" (").append(Thread.currentThread().getId()).append(')');
                System.out.println(sb.toString());
            }
        }

        ExecutorService exec = Executors.newFixedThreadPool(3);

        OrderedTaskWrapper<Integer> otw = new OrderedTaskWrapper<>();

        int[] seq = new int[10];
        System.out.println("keys:");
        for (int key = 0; key < seq.length; key++) {
            System.out.printf("%7d ", key);
        }
        System.out.println();
        for (int key = 0; key < seq.length; key++) {
            System.out.printf("------- ");
        }
        System.out.println();
        for (int i = 0; i < 100; i++) {
            int key = ThreadLocalRandom.current().nextInt(seq.length);
            exec.execute(otw.wrap(new Task(key, seq[key]++), key));
        }

        exec.shutdown();
    }
}