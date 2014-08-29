/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */
package si.pele.concurrent;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * A thin wrapper around {@link CompletableFuture} with a simpler API that attempts to handle
 * checked exceptions more gracefully than {@link java.util.concurrent.CompletionStage} and
 * provides a controllable facility to propagate {@link #cancel} command to dependencies (TODO).<p>
 * For example:
 * <pre>
 *
 * int result = Async.
 *      &lt;String, IOException>begin(() -> {
 *          if (Math.random() < 0.5d) {
 *              throw new FileNotFoundException();
 *          }
 *          return "12345";
 *      }).
 *      &lt;Integer, RuntimeException>then(r -> {
 *          try {
 *              return Integer.valueOf(r.get());
 *          } catch (IOException e) {
 *              throw new UncheckedIOException(e);
 *          }
 *      }).
 *      end();
 *
 * </pre>
 * <p/>
 * An {@link Async} class is a {@link Future} representing an asynchronous computation.
 * It has a fluent API for composition of dependent stages that depend on one or two
 * preceding stages. Starting stages are created using {@link #begin(Async.Beginning)}
 * or {@link #begin(Async.Beginning, Executor)} static methods, dependent stages are
 * created and attached using {@link #then(Async.Continuation)} or
 * {@link #then(Async.Continuation, Executor)}  instance methods.<p>
 * Since {@link Async} class is a {@link Future}, the constructed computation chain
 * can be manipulated using it's methods, but there are also convenience methods
 * {@link #end()} and {@link #end(long, TimeUnit)} that act like {@link Future#get()}
 * and {@link Future#get(long, TimeUnit)} respectively, unwrapping any possible
 * {@link ExecutionException} and wrapping {@link InterruptedException} and
 * {@link TimeoutException} into {@link Async.EndInterruptedException} and
 * {@link Async.EndTimeoutException} respectively so that they can be differentiated
 * from the exceptions thrown by the computation stages themselves which are thrown
 * unwrapped.
 *
 * @param <T> they type of result of the asynchronous computation
 * @param <E> they type of exception that might be thrown by the asynchronous computation
 */
public class Async<T, E extends Throwable> implements Future<T> {

    private final CompletableFuture<T> future;

    private Async(CompletableFuture<T> future) {
        this.future = future;
    }

    // Future<T> implementation...

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return future.isCancelled();
    }

    @Override
    public boolean isDone() {
        return future.isDone();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        return future.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return future.get(timeout, unit);
    }

    // static API

    // starting stage construction

    public static <R> Async<R, RuntimeException> beginSynchronously(R value) {
        return new Async<>(CompletableFuture.completedFuture(value));
    }

    public static <R, Eout extends Throwable> Async<R, Eout> begin(Beginning<R, Eout> supplier) {
        return new Async<>(CompletableFuture.supplyAsync(supplier));
    }

    public static <R, Eout extends Throwable> Async<R, Eout> begin(Beginning<R, Eout> supplier, Executor executor) {
        return new Async<>(CompletableFuture.supplyAsync(supplier, executor));
    }

    // instance API

    // single dependency

    public <R, Eout extends Throwable> Async<R, Eout> thenSynchronously(Continuation<? super T, E, ? extends R, Eout> fn) {
        return new Async<>(future.handle(fn));
    }

    public <R, Eout extends Throwable> Async<R, Eout> then(Continuation<? super T, E, ? extends R, Eout> fn) {
        return new Async<>(future.handleAsync(fn));
    }

    public <R, Eout extends Throwable> Async<R, Eout> then(Continuation<? super T, E, ? extends R, Eout> fn, Executor executor) {
        return new Async<>(future.handleAsync(fn, executor));
    }

    // double dependency (both complete then...)

    public <T2, E2 extends Throwable, R, Eout extends Throwable>
    Async<R, Eout> bothCompleteThenSynchronously(Async<T2, E2> that,
                                                 BiContinuation<T, E, T2, E2, R, Eout> fn) {
        BiCompletionStage<T, E, T2, E2, R, Eout> completion = new BiCompletionStage<>(fn);
        this.future.whenComplete(completion.bothCompleteHandler1());
        that.future.whenComplete(completion.bothCompleteHandler2());
        return new Async<>(completion);
    }

    public <T2, E2 extends Throwable, R, Eout extends Throwable>
    Async<R, Eout> bothCompleteThen(Async<T2, E2> that,
                                    BiContinuation<T, E, T2, E2, R, Eout> fn) {
        BiCompletionStage<T, E, T2, E2, R, Eout> completion = new BiCompletionStage<>(fn);
        this.future.whenCompleteAsync(completion.bothCompleteHandler1());
        that.future.whenCompleteAsync(completion.bothCompleteHandler2());
        return new Async<>(completion);
    }

    public <T2, E2 extends Throwable, R, Eout extends Throwable>
    Async<R, Eout> bothCompleteThen(Async<T2, E2> that,
                                    BiContinuation<T, E, T2, E2, R, Eout> fn,
                                    Executor executor) {
        BiCompletionStage<T, E, T2, E2, R, Eout> completion = new BiCompletionStage<>(fn);
        this.future.whenCompleteAsync(completion.bothCompleteHandler1(), executor);
        that.future.whenCompleteAsync(completion.bothCompleteHandler2(), executor);
        return new Async<>(completion);
    }

    // double dependency (first complete then...)

    public <T2, E2 extends Throwable, R, Eout extends Throwable>
    Async<R, Eout> firstCompleteThenSynchronously(Async<T2, E2> that,
                                                  BiContinuation<T, E, T2, E2, R, Eout> fn) {
        BiCompletionStage<T, E, T2, E2, R, Eout> completion = new BiCompletionStage<>(fn);
        this.future.whenComplete(completion.firstCompleteHandler1());
        that.future.whenComplete(completion.firstCompleteHandler2());
        return new Async<>(completion);
    }

    public <T2, E2 extends Throwable, R, Eout extends Throwable>
    Async<R, Eout> firstCompleteThen(Async<T2, E2> that,
                                     BiContinuation<T, E, T2, E2, R, Eout> fn) {
        BiCompletionStage<T, E, T2, E2, R, Eout> completion = new BiCompletionStage<>(fn);
        this.future.whenCompleteAsync(completion.firstCompleteHandler1());
        that.future.whenCompleteAsync(completion.firstCompleteHandler2());
        return new Async<>(completion);
    }

    public <T2, E2 extends Throwable, R, Eout extends Throwable>
    Async<R, Eout> firstCompleteThen(Async<T2, E2> that,
                                     BiContinuation<T, E, T2, E2, R, Eout> fn,
                                     Executor executor) {
        BiCompletionStage<T, E, T2, E2, R, Eout> completion = new BiCompletionStage<>(fn);
        this.future.whenCompleteAsync(completion.firstCompleteHandler1(), executor);
        that.future.whenCompleteAsync(completion.firstCompleteHandler2(), executor);
        return new Async<>(completion);
    }

    // double dependency (first success then...)

    public <T2, E2 extends Throwable, R, Eout extends Throwable>
    Async<R, Eout> firstSuccessThenSynchronously(Async<T2, E2> that,
                                                 BiContinuation<T, E, T2, E2, R, Eout> fn) {
        BiCompletionStage<T, E, T2, E2, R, Eout> completion = new BiCompletionStage<>(fn);
        this.future.whenComplete(completion.firstSuccessHandler1());
        that.future.whenComplete(completion.firstSuccessHandler2());
        return new Async<>(completion);
    }

    public <T2, E2 extends Throwable, R, Eout extends Throwable>
    Async<R, Eout> firstSuccessThen(Async<T2, E2> that,
                                    BiContinuation<T, E, T2, E2, R, Eout> fn) {
        BiCompletionStage<T, E, T2, E2, R, Eout> completion = new BiCompletionStage<>(fn);
        this.future.whenCompleteAsync(completion.firstSuccessHandler1());
        that.future.whenCompleteAsync(completion.firstSuccessHandler2());
        return new Async<>(completion);
    }

    public <T2, E2 extends Throwable, R, Eout extends Throwable>
    Async<R, Eout> firstSuccessThen(Async<T2, E2> that,
                                    BiContinuation<T, E, T2, E2, R, Eout> fn,
                                    Executor executor) {
        BiCompletionStage<T, E, T2, E2, R, Eout> completion = new BiCompletionStage<>(fn);
        this.future.whenCompleteAsync(completion.firstSuccessHandler1(), executor);
        that.future.whenCompleteAsync(completion.firstSuccessHandler2(), executor);
        return new Async<>(completion);
    }

    // waiting for computation result

    public T end() throws EndInterruptedException, E {
        try {
            return future.get();
        } catch (ExecutionException ee) {
            @SuppressWarnings("unchecked")
            E e = (E) ee.getCause();
            throw e;
        } catch (InterruptedException ie) {
            throw new EndInterruptedException(ie);
        }
    }

    public T end(long timeout, TimeUnit unit) throws EndInterruptedException, EndTimeoutException, E {
        try {
            return future.get(timeout, unit);
        } catch (ExecutionException ee) {
            @SuppressWarnings("unchecked")
            E e = (E) ee.getCause();
            throw e;
        } catch (InterruptedException ie) {
            throw new EndInterruptedException(ie);
        } catch (TimeoutException te) {
            throw new EndTimeoutException(te);
        }
    }

    // types

    public interface Beginning<T, E extends Throwable> extends Supplier<T> {

        T invoke() throws E;

        @Override
        default T get() {
            try {
                return invoke();
            } catch (Throwable e) {
                throw new CompletionException(e);
            }
        }
    }

    public interface Continuation<T, Ein extends Throwable, R, Eout extends Throwable> extends BiFunction<T, Throwable, R> {

        R invoke(Result<T, Ein> result) throws Eout;

        @Override
        default R apply(T t, Throwable e) {
            try {
                return invoke(Result.withUnwrappedException(t, e));
            } catch (Throwable eout) {
                throw new CompletionException(eout);
            }
        }
    }

    public interface BiContinuation<T1, Ein1 extends Throwable, T2, Ein2 extends Throwable, R, Eout extends Throwable> {

        R invoke(Result<T1, Ein1> result1, Result<T2, Ein2> result2) throws Eout;
    }

    public interface Result<T, E extends Throwable> {

        T get() throws E;

        boolean isSuccessful();

        static <T, E extends Throwable> Result<T, E> withUnwrappedException(T value, Throwable e) {
            @SuppressWarnings("unchecked")
            E exception = (e instanceof CompletionException && e.getCause() != null)
                          ? (E) e.getCause()
                          : (E) e;
            return with(value, exception);
        }

        static <T, E extends Throwable> Result<T, E> with(T value, E exception) {
            return exception == null
                   ?
                   new Result<T, E>() {
                       @Override
                       public T get() throws E { return value; }

                       @Override
                       public boolean isSuccessful() { return true; }
                   }
                   :
                   new Result<T, E>() {
                       @Override
                       public T get() throws E { throw exception; }

                       @Override
                       public boolean isSuccessful() { return false; }
                   };
        }
    }

    /**
     * What to do when {@link #cancel} is called on an {@link Async} instance?
     * Possible answers are: {@link #DONT_CASCADE}, {@link #CASCADE},
     * {@link #CASCADE_FORCE_INTERRUPT}, {@link #CASCADE_DONT_INTERRUPT}.
     */
    public static enum CancelMode {
        /**
         * Don't cascade {@link #cancel} to immediate dependencies (default)
         */
        DONT_CASCADE,
        /**
         * Cascade {@link #cancel} to immediate dependencies with same
         * {@code mayInterruptIfRunning} flag
         */
        CASCADE,
        /**
         * Cascade {@link #cancel} to immediate dependencies with
         * {@code mayInterruptIfRunning} flag forced to {@code true}
         */
        CASCADE_FORCE_INTERRUPT,
        /**
         * Cascade {@link #cancel} to immediate dependencies with
         * {@code mayInterruptIfRunning} flag forced to {@code false}
         */
        CASCADE_DONT_INTERRUPT
    }

    static class BiCompletionStage<T1, Ein1 extends Throwable, T2, Ein2 extends Throwable, R, Eout extends Throwable> extends CompletableFuture<R> {

        private static final Result<?, ?> DUMMY_RESULT = Result.with(null, null);

        private final BiContinuation<T1, Ein1, T2, Ein2, R, Eout> fn;
        private final AtomicReference<Result> resultExchange = new AtomicReference<>();

        BiCompletionStage(BiContinuation<T1, Ein1, T2, Ein2, R, Eout> fn) {
            this.fn = fn;
        }

        // handlers used for bothCompleteThen():

        BiConsumer<T1, Throwable> bothCompleteHandler1() {
            return (value1, e1) -> {
                Result<T1, Ein1> result1 = Result.withUnwrappedException(value1, e1);
                @SuppressWarnings("unchecked")
                Result<T2, Ein2> result2 = resultExchange.getAndSet(result1);
                if (result2 != null) { // both are complete now
                    resultExchange.lazySet(null); // release to GC
                    try {
                        complete(fn.invoke(result1, result2));
                    } catch (Throwable e) {
                        completeExceptionally(e);
                    }
                }
            };
        }

        BiConsumer<T2, Throwable> bothCompleteHandler2() {
            return (value2, e2) -> {
                Result<T2, Ein2> result2 = Result.withUnwrappedException(value2, e2);
                @SuppressWarnings("unchecked")
                Result<T1, Ein1> result1 = resultExchange.getAndSet(result2);
                if (result1 != null) { // both are complete now
                    resultExchange.lazySet(null); // release to GC
                    try {
                        complete(fn.invoke(result1, result2));
                    } catch (Throwable e) {
                        completeExceptionally(e);
                    }
                }
            };
        }

        // handlers used for firstCompleteThen():

        BiConsumer<T1, Throwable> firstCompleteHandler1() {
            return (value1, e1) -> {
                Result<T1, Ein1> result1 = Result.withUnwrappedException(value1, e1);
                if (resultExchange.getAndSet(DUMMY_RESULT) == null) { // we were the 1st
                    try {
                        complete(fn.invoke(result1, null));
                    } catch (Throwable e) {
                        completeExceptionally(e);
                    }
                }
            };
        }

        BiConsumer<T2, Throwable> firstCompleteHandler2() {
            return (value2, e2) -> {
                Result<T2, Ein2> result2 = Result.withUnwrappedException(value2, e2);
                if (resultExchange.getAndSet(DUMMY_RESULT) == null) { // we were the 1st
                    try {
                        complete(fn.invoke(null, result2));
                    } catch (Throwable e) {
                        completeExceptionally(e);
                    }
                }
            };
        }

        // handlers used for firstSuccessThen():

        BiConsumer<T1, Throwable> firstSuccessHandler1() {
            return (value1, e1) -> {
                Result<T1, Ein1> result1 = Result.withUnwrappedException(value1, e1);
                @SuppressWarnings("unchecked")
                Result<T2, Ein2> result2 = resultExchange.getAndSet(result1);
                if (// either we are successful and the other has not finished yet or was unsuccessful...
                    result1.isSuccessful() && (result2 == null || !result2.isSuccessful()) ||
                    // ... or we are second and both were unsuccessful...
                    !result1.isSuccessful() && result2 != null && !result2.isSuccessful()) {
                    try {
                        complete(fn.invoke(result1, result2));
                    } catch (Throwable e) {
                        completeExceptionally(e);
                    }
                }
                // release to GC only when second (to prevent double invocation)
                if (result2 != null) {
                    resultExchange.lazySet(null);
                }
            };
        }

        BiConsumer<T2, Throwable> firstSuccessHandler2() {
            return (value2, e2) -> {
                Result<T2, Ein2> result2 = Result.withUnwrappedException(value2, e2);
                @SuppressWarnings("unchecked")
                Result<T1, Ein1> result1 = resultExchange.getAndSet(result2);
                if (
                    // either we are successful and the other has not finished yet or was unsuccessful...
                    result2.isSuccessful() && (result1 == null || !result1.isSuccessful()) ||
                    // ... or we are second and both were unsuccessful...
                    !result2.isSuccessful() && result1 != null && !result1.isSuccessful()
                    ) {
                    try {
                        complete(fn.invoke(result1, result2));
                    } catch (Throwable e) {
                        completeExceptionally(e);
                    }
                }
                // release to GC only when second (to prevent double invocation)
                if (result1 != null) {
                    resultExchange.lazySet(null);
                }
            };
        }
    }

    public static class EndException extends Exception {
        EndException(Throwable cause) {
            super(cause);
        }
    }

    public static class EndInterruptedException extends EndException {
        public EndInterruptedException(InterruptedException cause) {
            super(cause);
        }

        @Override
        public InterruptedException getCause() {
            return (InterruptedException) super.getCause();
        }
    }

    public static class EndTimeoutException extends EndException {
        public EndTimeoutException(TimeoutException cause) {
            super(cause);
        }

        @Override
        public TimeoutException getCause() {
            return (TimeoutException) super.getCause();
        }
    }

    // Examples

    private static class AppException extends Exception {
        AppException(Throwable cause) {
            super(cause);
        }
    }

    private static void example1() {

        Async<Integer, AppException> cf = Async.
            <String, IOException>begin(() -> {
                if (Math.random() < 0.5d) {
                    throw new FileNotFoundException();
                }
                return "12345";
            }).then(r -> {
            try {
                return Integer.valueOf(r.get());
            } catch (IOException e) {
                throw new AppException(e);
            }
        });

        try {
            System.out.println(cf.end());
        } catch (AppException | EndException e) {
            e.printStackTrace();
        }
    }

    private static void example2() {

        Async<String, FileNotFoundException> cf1 = Async.begin(() -> {
            if (Math.random() < 0.5d) {
                throw new FileNotFoundException();
            }
            return "01234";
        });

        Async<String, EOFException> cf2 = Async.begin(() -> {
            if (Math.random() < 0.5d) {
                throw new EOFException();
            }
            return "56789";
        });

        Async<String, AppException> cf3 =
            cf1.firstSuccessThen(cf2, (r1, r2) -> {
                assert r1 != null || r2 != null;
                AppException ae = null;
                try {
                    if (r1 != null) return r1.get();
                } catch (FileNotFoundException e) {
                    ae = new AppException(e);
                }
                try {
                    if (r2 != null) return r2.get();
                } catch (EOFException e) {
                    if (ae == null)
                        ae = new AppException(e);
                    else
                        ae.addSuppressed(e);
                }
                throw ae;
            });

        try {
            System.out.println(cf3.end());
        } catch (AppException | EndException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        example2();
    }
}
