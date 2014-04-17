/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */
package si.pele.concurrent.queue;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.AbstractQueue;
import java.util.Queue;
import java.util.function.Supplier;

/**
 * A {@link Queue} extension that can {@link #offerNode}/{@link #pollNode}
 * in addition to {@link #offer}ing/{@link #poll}ing the elements in order
 * to optimize transferring elements from one {@link NQueue} to another
 * without producing garbage.
 *
 * @author peter.levart@gmail.com
 */
public interface NQueue<E> extends Queue<E> {

    /**
     * A linked node used in {@link NQueue}s for holding elements and
     * organizing linked lists.
     *
     * @param <E> the type of element contained in the node
     */
    final class Node<E> implements Supplier<E> {

        // Unsafe machinery

        static final Unsafe U;

        static {
            try {
                Field f = Unsafe.class.getDeclaredField("theUnsafe");
                f.setAccessible(true);
                U = (Unsafe) f.get(null);
            } catch (IllegalAccessException e) {
                throw (Error) new IllegalAccessError(e.getMessage()).initCause(e);
            } catch (NoSuchFieldException e) {
                throw (Error) new NoSuchFieldError(e.getMessage()).initCause(e);
            }
        }

        static long fieldOffset(Class<?> clazz, String fieldName) {
            try {
                return U.objectFieldOffset(clazz.getDeclaredField(fieldName));
            } catch (NoSuchFieldException e) {
                throw (Error) new NoSuchFieldError(clazz.getName() + "." + fieldName).initCause(e);
            }
        }

        // constructors

        public Node(E e) {
            if (e == null) throw new NullPointerException();
            element = e;
        }

        Node() {}

        // element

        private E element;
        private static final long elementOffset = fieldOffset(Node.class, "element");

        public E get() { return element; }

        void put(E e) { element = e; }

        void puto(E e) { U.putOrderedObject(this, elementOffset, e); }

        // next

        private volatile Node<E> next;
        private static final long nextOffset = fieldOffset(Node.class, "next");

        Node<E> getvNext() { return next; }

        void putvNext(Node<E> n) { next = n; }

        void putoNext(Node<E> n) { U.putOrderedObject(this, nextOffset, n); }
    }

    boolean offerNode(Node<E> n);

    Node<E> pollNode();

    /**
     * a base class for implementing {@link NQueue}s
     */
    abstract class Base<E> extends AbstractQueue<E> implements NQueue<E> {

        public boolean offer(E e) {
            return offerNode(new Node<>(e));
        }

        public E poll() {
            Node<E> n = pollNode();
            return (n == null) ? null : n.get();
        }
    }
}
