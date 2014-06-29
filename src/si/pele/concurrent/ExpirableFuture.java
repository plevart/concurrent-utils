/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */
package si.pele.concurrent;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * A {@link Future} which expires if the underlying computation does not commence
 * execution before {@link #deadline()} is reached (measured in terms of
 * {@link System#nanoTime()}). Expired {@link ExpirableFuture}s report true by
 * {@link #isExpired()} and throw {@link ExecutionException} wrapping
 * {@link ExpiredException} by {@link #get()}.<p>
 *
 * @author peter.levart@gmail.com
 */
public interface ExpirableFuture<V> extends Future<V>, Comparable<ExpirableFuture<?>> {

    /**
     * @return {@code true} if this {@link ExpirableFuture} has expired because
     *         the underlying computation has not commenced execution before
     *         {@link #deadline() deadline}.
     */
    boolean isExpired();

    /**
     * @return the time (measured in terms of {@link System#nanoTime()}) before
     *         which the underlying computation has to commence execution or it will be
     *         {@link #isExpired() expired}.
     */
    long deadline();

    /**
     * By default {@link ExpirableFuture}s are comparable among themselves
     * with a natural order of their ascending {@link #deadline()}.
     *
     * @param other the other {@link ExpirableFuture} to compare {@code this}
     *              with.
     */
    default int compareTo(ExpirableFuture<?> other) {
        return Long.compare(this.deadline(), other.deadline());
    }

    /**
     * Expired {@link ExpirableFuture}s throw {@link ExecutionException}
     * wrapping {@link ExpiredException} from their
     * {@link ExpirableFuture#get()} method.
     */
    final class ExpiredException extends Exception {}
}
