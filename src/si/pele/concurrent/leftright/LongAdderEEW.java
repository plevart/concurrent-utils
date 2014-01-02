package si.pele.concurrent.leftright;

import java.util.concurrent.atomic.LongAdder;

/**
 * @author peter
 */
class LongAdderEEW implements EnterExitWait {
    private final LongAdder enters = new LongAdder();
    private final LongAdder exits = new LongAdder();

    public void enter() {
        enters.increment();
    }

    public void exit() {
        exits.increment();
    }

    public void waitEmpty() {
        while (exits.sum() != enters.sum()) {
            Thread.yield();
        }
    }
}
