package si.pele.concurrent.leftright;

import sun.misc.Unsafe;

import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author peter
 */
public class TLChainEEW implements EnterExitWait {

    private final AtomicReference<Entry> first = new AtomicReference<>();
    private final ThreadLocal<Entry> tl = new ThreadLocal<Entry>() {
        @Override
        protected Entry initialValue() {
            Entry e = new Entry();
            Entry f;
            do {
                f = first.get();
                e.next = f;
            } while (!first.compareAndSet(f, e));
            return e;
        }
    };

    private static final class Entry extends WeakReference<Thread> {
        private static final Unsafe U;
        private static final long inOffset;

        static {
            try {
                Field uf = Unsafe.class.getDeclaredField("theUnsafe");
                uf.setAccessible(true);
                U = (Unsafe) uf.get(null);
                inOffset = U.objectFieldOffset(Entry.class.getDeclaredField("in"));
            } catch (Exception e) {
                throw new Error(e);
            }
        }

        Entry() { super(Thread.currentThread()); }

        Entry next;

        // padding before 'in' to prevent cache-line false-sharing (128 bytes)
        private int
            _00, _01, _02, _03, _04, _05, _06, _07,
            _10, _11, _12, _13, _14, _15, _16, _17,
            _20, _21, _22, _23, _24, _25, _26, _27,
            _30, _31, _32, _33, _34, _35, _36, _37;

        volatile int in;

        // 'in' field is frequently updated by reader thread. To ensure sequential consistency
        // we might only need to ensure that updates are ordered, nothing more. Correct?
        void setIn(int in) {
            U.putOrderedInt(this, inOffset, in);
        }

        // padding after 'in' to prevent cache-line false-sharing (128 bytes)
        private int
            _40, _41, _42, _43, _44, _45, _46, _47,
            _50, _51, _52, _53, _54, _55, _56, _57,
            _60, _61, _62, _63, _64, _65, _66, _67,
            _70, _71, _72, _73, _74, _75, _76, _77;

    }

    @Override
    public void enter() {
        tl.get().setIn(1);
    }

    @Override
    public void exit() {
        tl.get().setIn(0);
    }

    @Override
    public void waitEmpty() {
        retry:
        while (true) {
            // remember 1st
            Entry f = first.get();
            Entry d = null;
            for (Entry e = f; e != null; e = e.next) {
                if (e.get() == null) {
                    // entry is cleared, thread is gone -> expunge
                    if (d == null) {
                        if (first.compareAndSet(f, e.next)) {
                            f = e.next;
                        } else {
                            // first changed concurrently -> retry loop
                            continue retry;
                        }
                    } else {
                        d.next = e.next;
                    }
                } else {
                    while (e.in > 0) {
                        Thread.yield();
                    }
                    d = e;
                }
            }
            break;
        }
    }
}
