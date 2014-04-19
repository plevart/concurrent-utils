/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */
package si.pele.concurrent.queue;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.function.Supplier;

/**
 * A linked node used in {@link java.util.Queue}s for holding elements and
 * organizing linked lists.
 *
 * @param <E> the type of element contained in the node
 * @author peter.levart@gmail.com
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

    Node(E e) {
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
