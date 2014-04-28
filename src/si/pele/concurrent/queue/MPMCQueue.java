/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */
package si.pele.concurrent.queue;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

import static si.pele.concurrent.queue.Node.U;
import static si.pele.concurrent.queue.Node.fieldOffset;

/**
 * Unbounded Multiple Producer Multiple Consumer {@link java.util.Queue} implementation
 * using linked list.
 *
 * @author peter.levart@gmail.com
 */
public class MPMCQueue<E> extends AbstractQueue<E> {

    private Object
        pad00, pad01, pad02, pad03, pad04, pad05, pad06, pad07,
        pad08, pad09, pad0A, pad0B, pad0C, pad0D, pad0E, pad0F;

    private volatile Node<E> tail = new Node<>();

    private Object
        pad10, pad11, pad12, pad13, pad14, pad15, pad16, pad17,
        pad18, pad19, pad1A, pad1B, pad1C, pad1D, pad1E, pad1F;

    private volatile Node<E> head = tail;

    private Object
        pad20, pad21, pad22, pad23, pad24, pad25, pad26, pad27,
        pad28, pad29, pad2A, pad2B, pad2C, pad2D, pad2E, pad2F;

    private static final long tailOffset = fieldOffset(MPMCQueue.class, "tail");

    private boolean casTail(Node<E> oldTail, Node<E> newTail) {
        return U.compareAndSwapObject(this, tailOffset, oldTail, newTail);
    }

    private static final long headOffset = fieldOffset(MPMCQueue.class, "head");

    @SuppressWarnings("unchecked")
    private Node<E> gasHead(Node<E> newHead) {
        return (Node<E>) U.getAndSetObject(this, headOffset, newHead);
    }

    final Node<E> putNode(E e) {
        Node<E> n = new Node<>(e);
        Node<E> h = gasHead(n);
        h.putvNext(n);
        return n;
    }

    @Override
    public boolean offer(E e) {
        putNode(e);
        return true;
    }

    @Override
    public E poll() {
        Node<E> t, n;
        E e;
        do {
            do {
                t = tail;
                n = t.getvNext();
                if (n == null) return null;
            } while (!casTail(t, n));
        } while ((e = n.gas(null)) == null);
        return e;
    }

    @Override
    public E peek() {
        Node<E> t, n;
        E e;
        t = tail;
        while (true) {
            n = t.getvNext();
            if (n == null) return null;
            if ((e = n.get()) != null) return e;
            if (casTail(t, n)) {
                t = n;
            } else {
                t = tail;
            }
        }
    }

    @Override
    public int size() {
        int size = 0;
        for (Node<E> n = tail.getvNext(); n != null; n = n.getvNext()) {
            if (n.get() != null) {
                size++;
            }
        }
        return size;
    }

    @Override
    public boolean contains(Object o) {
        if (o == null) return false;
        for (Node<E> n = tail.getvNext(); n != null; n = n.getvNext()) {
            E e = n.get();
            if (e != null && e.equals(o)) {
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
        private Node<E> l, n;
        private E ne;

        It(Node<E> n) {
            this.n = n;
        }

        @Override
        public boolean hasNext() {
            while (n != null && (ne = n.get()) == null) {
                n = n.getvNext();
            }
            return n != null;
        }

        @Override
        public E next() {
            if (!hasNext()) throw new NoSuchElementException();
            E e = ne;
            l = n;
            n = n.getvNext();
            ne = null;
            return e;
        }

        @Override
        public void remove() {
            if (l == null) throw new IllegalStateException();
            l.putv(null);
            l = null;
        }
    }

    @Override
    public boolean remove(Object o) {
        if (o == null) return false;
        for (Node<E> n = tail.getvNext(); n != null; n = n.getvNext()) {
            E e = n.get();
            if (e != null && e.equals(o) && n.cas(e, null)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void clear() {
        Node<E> n = new Node<>();
        // start accumulating new nodes on head 1st...
        head = n;
        // ...then change the view (this is important to not loose any offers)
        tail = n;
    }

    // this is internal iterator which removes cleared nodes on the way...

    @Override
    public void forEach(Consumer<? super E> action) {
        Node<E> p = null;
        Node<E> t = tail;
        Node<E> n = t.getvNext();
        while (n != null) {
            E e = n.get();
            if (e == null) {
                if (p == null) {
                    if (casTail(t, n)) {
                        t = n;
                    } else {
                        t = tail;
                    }
                } else {
                    if (p.casNext(t, n)) {
                        t = n;
                    } else {
                        t = p.getvNext();
                    }
                }
            } else {
                action.accept(e);
                p = t;
                t = n;
            }
            n = t.getvNext();
        }
    }

    /**
     * A bounded variant of {@link si.pele.concurrent.queue.MPMCQueue} implemented using ingress/egress
     * counters.
     */
    public static class Bounded<E> extends MPMCQueue<E> implements BoundedQueue<E> {

        private final int capacity;
        private final LongAdder ingressCount = new LongAdder();
        private final LongAdder egressCount = new LongAdder();

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
            if (e != null) egressCount.increment();
            return e;
        }

        @Override
        public int size() {
            // read egress before ingress - this returns a conservative upper bound of current size
            return (int) (-egressCount.sum() + ingressCount.sum());
        }

        @Override
        public int capacity() {
            return capacity;
        }

        /**
         * A {@link BlockingQueue} variant of {@link MPMCQueue.Bounded} implemented by spin/yield
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
     * A {@link BlockingQueue} variant of {@link MPMCQueue} (unbounded) implemented by
     * spin/yield back-off-based loops.
     */
    public static class Yielding<E> extends MPMCQueue<E> implements YieldingQueue<E> {
        public Yielding() { }
    }

    /**
     * A {@link BlockingQueue} variant of {@link MPMCQueue} (unbounded) implemented by
     * park/unparking the consumer threads when queue is empty.
     */
    public static class Parking<E> extends MPMCQueue<E> implements YieldingQueue<E> {

        private final MPMCQueue<Thread> consumers = new MPMCQueue<>();

        @Override
        public boolean offer(E e) {
            if (super.offer(e)) {
                consumers.forEach(LockSupport::unpark);
                return true;
            } else {
                return false;
            }
        }

        @Override
        public E take() throws InterruptedException {
            int c = 0;
            E e;
            while ((e = poll()) == null && ++c < SPINS) {
                if (Thread.interrupted()) throw new InterruptedException();
            }
            if (e != null) {
                return e;
            }
            Node<Thread> us = consumers.putNode(Thread.currentThread());
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
                us.puto(null);
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
            if (e != null) {
                return e;
            }
            Node<Thread> us = consumers.putNode(Thread.currentThread());
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
                us.puto(null);
            }
        }
    }
}
