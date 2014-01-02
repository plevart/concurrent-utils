package si.pele.concurrent.leftright;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class LeftRight<T> {
    private volatile T leftState;
    private volatile EnterExitWait leftReaders;

    private T rightState;
    private EnterExitWait rightReaders;

    private final Object lock = new Object();

    public LeftRight(T leftState, T rightState) {
        this(leftState, new LongAdderEEW(), rightState, new LongAdderEEW());
    }

    public LeftRight(T leftState, EnterExitWait leftReaders, T rightState, EnterExitWait rightReaders) {
        this.leftState = leftState;
        this.leftReaders = leftReaders;
        this.rightState = rightState;
        this.rightReaders = rightReaders;
    }

    public <R> R read(Function<? super T, R> readFunction) {
        return read(readFunction, Function::apply);
    }

    public void modify(Consumer<? super T> modifyConsumer) {
        modify(modifyConsumer, Consumer::accept);
    }

    public <R, P> R read(P param, BiFunction<? super P, ? super T, R> readFunction) {
        // read leftReaders 1st
        EnterExitWait lr = leftReaders;
        lr.enter();
        try {
            // read leftState 2nd
            return readFunction.apply(param, leftState);
        } finally {
            lr.exit();
        }
    }

    public <P> void modify(P param, BiConsumer<? super P, ? super T> modifyConsumer) {
        synchronized (lock) {
            // modify rightState
            T rs = rightState;
            modifyConsumer.accept(param, rs);
            // swap left<->right state 1st
            T ls = leftState;
            rightState = ls;
            leftState = rs;
            // wait for rightReaders to complete
            EnterExitWait rr = rightReaders;
            rr.waitEmpty();
            // swap left<->right readers 2nd
            EnterExitWait lr = leftReaders;
            rightReaders = lr;
            leftReaders = rr;
            // wait for ex-leftReaders (now-rightReaders) to complete
            lr.waitEmpty();
            // modify ex-leftState (now-rightState)
            modifyConsumer.accept(param, ls);
        }
    }
}
