/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */
package si.pele.concurrent.queue;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;

import static si.pele.concurrent.queue.Node.U;
import static si.pele.concurrent.queue.Node.fieldOffset;

/**
 * Unbounded Multiple Producer Single Consumer {@link Queue} implementation
 * using linked list.
 *
 * @author peter.levart@gmail.com
 */
public class MPSCQueue<E> extends AbstractQueue<E> {

    private Object
        pad00, pad01, pad02, pad03, pad04, pad05, pad06, pad07,
        pad08, pad09, pad0A, pad0B, pad0C, pad0D, pad0E, pad0F;

    private Node<E> tail = new Node<>();

    private Object
        pad10, pad11, pad12, pad13, pad14, pad15, pad16, pad17,
        pad18, pad19, pad1A, pad1B, pad1C, pad1D, pad1E, pad1F;

    private volatile Node<E> head = tail;

    private Object
        pad20, pad21, pad22, pad23, pad24, pad25, pad26, pad27,
        pad28, pad29, pad2A, pad2B, pad2C, pad2D, pad2E, pad2F;

    private static final long headOffset = fieldOffset(MPSCQueue.class, "head");

    @SuppressWarnings("unchecked")
    private Node<E> gasHead(Node<E> newHead) {
        return (Node<E>) U.getAndSetObject(this, headOffset, newHead);
    }

    @Override
    public boolean offer(E e) {
        Node<E> n = new Node<>(e);
        Node<E> h = gasHead(n);
        h.putvNext(n);
        return true;
    }

    @Override
    public E poll() {
        Node<E> t = tail;
        Node<E> n = t.getvNext();
        if (n == null) return null;
        E e = n.get();
        n.puto(null); // for GC
        tail = n;
        return e;
    }

    @Override
    public E peek() {
        Node<E> n = tail.getvNext();
        return (n == null) ? null : n.get();
    }

    @Override
    public int size() {
        int size = 0;
        for (Node<E> n = tail.getvNext(); n != null; n = n.getvNext()) {
            size++;
        }
        return size;
    }

    @Override
    public boolean isEmpty() {
        return tail.getvNext() == null;
    }

    @Override
    public boolean contains(Object o) {
        if (o == null) return false;
        for (Node<E> n = tail.getvNext(); n != null; n = n.getvNext()) {
            if (n.get().equals(o)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Iterator<E> iterator() {
        return new It<>(tail.getvNext());
    }

    private static class It<E> implements Iterator<E> {
        private Node<E> n;

        It(Node<E> n) {
            this.n = n;
        }

        @Override
        public boolean hasNext() {
            return n != null;
        }

        @Override
        public E next() {
            if (n == null) throw new NoSuchElementException();
            E e = n.get();
            if (e == null) throw new ConcurrentModificationException();
            n = n.getvNext();
            return e;
        }
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeIf(Predicate<? super E> filter) {
        throw new UnsupportedOperationException();
    }

    /**
     * This is considered to be a "Consumer" operation, which means it should
     * only be called from consumer thread of this MPSC queue.
     */
    @Override
    public void clear() {
        head = tail = new Node<>();
    }

    /**
     * A bounded variant of {@link MPSCQueue} implemented using ingress/egress
     * counters.
     */
    public static class Bounded<E> extends MPSCQueue<E> implements BoundedQueue<E> {

        private final int capacity;

        private final LongAdder ingressCount = new LongAdder();

        private long
            pad00, pad01, pad02, pad03, pad04, pad05, pad06, pad07;

        private volatile long egressCount;

        private long
            pad10, pad11, pad12, pad13, pad14, pad15, pad16, pad17;

        private static final long egressCountOffset = fieldOffset(Bounded.class, "egressCount");

        private void putoEgressCount(long c) {
            U.putOrderedLong(this, egressCountOffset, c);
        }

        public Bounded(int capacity) {
            this.capacity = capacity;
        }

        @Override
        public boolean offer(E e) {
            if (size() >= capacity) return false;
            boolean r = super.offer(e);
            if (r) ingressCount.increment();
            return r;
        }

        @Override
        public E poll() {
            E e = super.poll();
            if (e != null) putoEgressCount(egressCount + 1L);
            return e;
        }

        @Override
        public int size() {
            // read egress before ingress - this returns a conservative upper bound of current size
            return (int) (-egressCount + ingressCount.sum());
        }

        @Override
        public int capacity() {
            return capacity;
        }

        /**
         * A {@link BlockingQueue} variant of {@link MPSCQueue.Bounded} implemented by spin/yield
         * back-off-based loops.
         */
        public static class Yielding<E> extends Bounded<E> implements YieldingQueue<E> {
            public Yielding(int capacity) {
                super(capacity);
            }

            @Override
            public int remainingCapacity() {
                return super.remainingCapacity();
            }
        }
    }

    /**
     * A {@link BlockingQueue} variant of {@link MPSCQueue} (unbounded) implemented by
     * spin/yield back-off-based loops.
     */
    public static class Yielding<E> extends MPSCQueue<E> implements YieldingQueue<E> {
        public Yielding() { }
    }

    /**
     * A {@link BlockingQueue} variant of {@link MPSCQueue} (unbounded) implemented by
     * park/unparking the consumer thread when queue us empty.
     */
    public static class Parking<E> extends MPSCQueue<E> implements YieldingQueue<E> {

        private volatile Thread consumer;

        private static final long consumerOffset = fieldOffset(Parking.class, "consumer");

        private void putoConsumer(Thread t) {
            U.putOrderedObject(this, consumerOffset, t);
        }

        @Override
        public void put(E e) throws InterruptedException {
            YieldingQueue.super.put(e);
            unparkConsumer();
        }

        @Override
        public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
            if (YieldingQueue.super.offer(e, timeout, unit)) {
                unparkConsumer();
                return true;
            } else {
                return false;
            }
        }

        private void unparkConsumer() {
            Thread t = consumer;
            if (t != null) {
                LockSupport.unpark(t);
            }
        }

        @Override
        public E take() throws InterruptedException {
            int c = 0;
            E e;
            while ((e = poll()) == null && ++c < SPINS) {
                if (Thread.interrupted()) throw new InterruptedException();
            }
            if (e != null) return e;
            consumer = Thread.currentThread();
            try {
                while (true) {
                    if (Thread.interrupted()) throw new InterruptedException();
                    if ((e = poll()) == null) {
                        LockSupport.park(this);
                    } else {
                        return e;
                    }
                }
            } finally {
                putoConsumer(null);
            }
        }

        @Override
        public E poll(long timeout, TimeUnit unit) throws InterruptedException {
            long deadline = System.nanoTime() + unit.toNanos(timeout);
            int c = 0;
            E e;
            while ((e = poll()) == null && ++c < SPINS) {
                if (Thread.interrupted()) throw new InterruptedException();
                if (System.nanoTime() >= deadline) return null;
            }
            if (e != null) return e;
            consumer = Thread.currentThread();
            try {
                while (true) {
                    if (Thread.interrupted()) throw new InterruptedException();
                    long nanos = deadline - System.nanoTime();
                    if (nanos <= 0) return null;
                    if ((e = poll()) == null) {
                        LockSupport.parkNanos(this, nanos);
                    } else {
                        return e;
                    }
                }
            } finally {
                putoConsumer(null);
            }
        }
    }
}
