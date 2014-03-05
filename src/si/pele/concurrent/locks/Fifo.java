/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */
package si.pele.concurrent.locks;

/**
 * @author peter
 */
class Fifo<T> {
    private Fifo<T> next, prev;
    private final T element;

    Fifo() {
        next = prev = this;
        element = null;
    }

    private Fifo(T element) {
        this.element = element;
    }

    Fifo<T> insertAfter(T element) {
        Fifo<T> fifo = new Fifo<>(element);
        synchronized (this) {
            synchronized (next) {
                (fifo.next = next).prev = fifo;
                (fifo.prev = this).next = fifo;
            }
        }
        return fifo;
    }

    Fifo<T> insertBefore(T element) {
        Fifo<T> fifo = new Fifo<>(element);
        synchronized (this) {
            synchronized (next) {
                (fifo.next = next).prev = fifo;
                (fifo.prev = this).next = fifo;
            }
        }
        return fifo;
    }

}
