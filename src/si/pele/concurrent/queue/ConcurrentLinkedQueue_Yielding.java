/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */
package si.pele.concurrent.queue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A {@link BlockingQueue} variant of {@link ConcurrentLinkedQueue} (unbounded)
 * implemented by spin/yield  back-off-based loops.
 *
 * @author peter.levart@gmail.com
 */
public class ConcurrentLinkedQueue_Yielding<E>
    extends ConcurrentLinkedQueue<E>
    implements YieldingQueue<E> {
}
