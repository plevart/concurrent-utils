/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */
package si.pele.concurrent.queues;

import sun.misc.Unsafe;

import java.util.Queue;
import java.util.function.Supplier;

/**
 * A {@link Queue} extension that can {@link #offerNode}/{@link #pollNode} in
 * addition to plain elements. This can be used to optimize transfer from one
 * {@link NQueue} to another without producing intermediary garbage...
 *
 * @author peter.levart@gmail.com
 */
public interface NQueue<E> extends Queue<E> {

    final class Node<E> implements Supplier<E> {

        // Unsafe machinery

        static final sun.misc.Unsafe U;

        static {
            try {
                java.lang.reflect.Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
                f.setAccessible(true);
                U = (Unsafe) f.get(null);
            } catch (Exception e) {
                throw new Error(e);
            }
        }

        static long fieldOffset(Class<?> clazz, String fieldName) {
            try {
                return U.objectFieldOffset(clazz.getDeclaredField(fieldName));
            } catch (NoSuchFieldException e) {
                throw new NoSuchFieldError(fieldName);
            }
        }

        // element

        private E element;

        public Node(E e) {
            if (e == null) throw new NullPointerException();
            element = e;
        }

        Node() {}

        public E get() { return element; }

        void put(E e) { element = e; }

        private static final long elementOffset = fieldOffset(Node.class, "element");

        void putOrdered(E e) {
            U.putOrderedObject(this, elementOffset, e);
        }

        // next

        volatile Node<E> next;

        private static final long nextOffset = fieldOffset(Node.class, "next");

        void putOrderedNext(Node<E> n) {
            U.putOrderedObject(this, nextOffset, n);
        }
    }

    boolean offerNode(Node<E> n);

    Node<E> pollNode();

    default boolean offer(E e) {
        return offerNode(new Node<>(e));
    }

    default E poll() {
        Node<E> n = pollNode();
        return (n == null) ? null : n.get();
    }

    // defaults taken from AbstractQueue
}
