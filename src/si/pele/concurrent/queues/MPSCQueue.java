/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */
package si.pele.concurrent.queues;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

import static si.pele.concurrent.queues.NQueue.Node.U;
import static si.pele.concurrent.queues.NQueue.Node.fieldOffset;

/**
 * @author peter.levart@gmail.com
 */
public class MPSCQueue<E> extends AbstractQueue<E> implements NQueue<E> {

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
    public boolean offerNode(Node<E> n) {
        if (n == null) throw new NullPointerException();
        Node<E> h = gasHead(n);
        h.next = n;
        return true;
    }

    @Override
    public Node<E> pollNode() {
        Node<E> t = tail;
        Node<E> n = t.next;
        if (n == null) return null;
        E e = n.get();
        n.putOrdered(null); // for GC
        tail = n;
        // re-use 't' as a container
        t.put(e);
        t.putOrderedNext(null); // detach next
        return t;
    }

    @Override
    public E peek() {
        Node<E> n = tail.next;
        return (n == null) ? null : n.get();
    }

    @Override
    public int size() {
        int size = 0;
        for (Node<E> n = tail.next; n != null; n = n.next) {
            size++;
        }
        return size;
    }

    @Override
    public boolean isEmpty() {
        return tail.next == null;
    }

    @Override
    public boolean contains(Object o) {
        for (Node<E> n = tail.next; n != null; n = n.next) {
            if (n.get().equals(o)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Iterator<E> iterator() {
        return new It<>(tail.next);
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
            n = n.next;
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

    @Override
    public void clear() {
        head = tail = new Node<>();
    }

}
