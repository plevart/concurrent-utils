package si.pele.concurrent.leftright;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * @author peter
 */
public class LRTest implements Runnable {

    public static void main(String[] args) {
        @SuppressWarnings("unchecked")
        Supplier<EnterExitWait>[] eewSuppliers = new Supplier[]{
            TLChainEEW::new,
            TLChainVolatileEEW::new,
            LongAdderEEW::new
        };

        int[] ns = {128, 1024, 1024 * 1024};

        System.out.println("\nWarm-up:\n");
        for (Supplier<EnterExitWait> eewSupplier : eewSuppliers) {
            System.out.println(eewSupplier.get().getClass().getSimpleName() + ":");
            new LRTest(1024, 2, eewSupplier).run();
        }

        System.out.println("\nMeasure:\n");
        for (int n : ns) {
            for (int rthreads = 1; rthreads <= Runtime.getRuntime().availableProcessors() - 2; rthreads++) {
                for (Supplier<EnterExitWait> eewSupplier : eewSuppliers) {
                    System.out.println(eewSupplier.get().getClass().getSimpleName() + ":");
                    new LRTest(n, rthreads, eewSupplier).run();
                }
            }
            System.out.println();
        }
    }

    final int n;
    final int quarter;
    final int readerThreads;
    final Integer[] elements;
    final LeftRight<Set<Integer>> lrSet;

    public LRTest(int n, int readerThreads, Supplier<EnterExitWait> eewSupplier) {
        n &= ~7; // ensure n is dividable by 8
        this.n = n;
        this.quarter = n >> 2;
        this.readerThreads = readerThreads;
        Set<Integer> uniqueElements = new HashSet<>((n * 4 + 2) / 3);
        while (uniqueElements.size() < n) {
            uniqueElements.add(ThreadLocalRandom.current().nextInt());
        }
        elements = uniqueElements.toArray(new Integer[n]);
        // create lrSet
        lrSet = new LeftRight<Set<Integer>>(new HashSet<>(), eewSupplier.get(), new HashSet<>(), eewSupplier.get());
        // pre-populate lrSet with 1/4th of elements
        lrSet.modify(
            set -> {
                for (int i = 0; i < quarter; i++) {
                    set.add(elements[i]);
                }
            }
        );
    }

    private static final long rampUpMillis = 1000L;
    private static final long runMillis = 10000L;
    private static final long rampDownMillis = 200L;

    @Override
    public void run() {
        AtomicInteger latch = new AtomicInteger(readerThreads + 2); // +2 Writers
        Worker[] workers = new Worker[readerThreads + 2];
        for (int i = 0; i < readerThreads; i++) {
            workers[i] = new Reader(latch);
        }
        workers[readerThreads] = new Writer(latch, false);
        workers[readerThreads + 1] = new Writer(latch, true);
        for (Worker worker : workers) {
            worker.start();
        }
        // ramp-up (warm-up) phase
        try { Thread.sleep(rampUpMillis); } catch (InterruptedException e) { }
        latch.incrementAndGet();
        // count-ops phase
        try { Thread.sleep(runMillis); } catch (InterruptedException e) { }
        latch.incrementAndGet();
        // ramp-down phase
        try { Thread.sleep(rampDownMillis); } catch (InterruptedException e) { }
        latch.incrementAndGet();
        // collect results;
        for (Worker worker : workers) {
            try {
                worker.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        long readOpsSum = 0;
        for (int i = 0; i < readerThreads; i++) {
            readOpsSum += workers[i].ops;
        }
        double readOpsAvg = (double) readOpsSum / readerThreads;
        double avgReadOpNanos = (double) runMillis * 1000_000L / readOpsAvg;
        double readOpsSumPerSecond = (double) readOpsSum * 1000L / runMillis;

        long modifyOpsSum = 0;
        for (int i = readerThreads; i < workers.length; i++) {
            modifyOpsSum += workers[i].ops;
        }
        double modifyOpsAvg = (double) modifyOpsSum / (workers.length - readerThreads);
        double avgModifyOpNanos = (double) runMillis * 1000_000L / modifyOpsAvg;
        double modifyOpsSumPerSecond = (double) modifyOpsSum * 1000L / runMillis;

        System.out.printf(
            "%8d elements, %3d reader / %3d writer threads: cumulative throughput %12.2f reads/s, avg read %8.2f ns, cumulative throughput %12.2f writes/s, avg write %8.2f ns\n",
            n, readerThreads, (workers.length - readerThreads), readOpsSumPerSecond, avgReadOpNanos, modifyOpsSumPerSecond, avgModifyOpNanos
        );
    }

    static class Worker extends Thread {
        final AtomicInteger latch;

        Worker(AtomicInteger latch) {
            this.latch = latch;
        }

        long ops;
    }

    class Reader extends Worker {

        Reader(AtomicInteger latch) {
            super(latch);
        }

        @Override
        public void run() {
            // decrement and wait for all threads to arrive
            latch.decrementAndGet();
            while (latch.get() > 0L) {}

            // initial random index into elements array
            int i = ThreadLocalRandom.current().nextInt(elements.length);

            // cache outer instance variables in locals
            final Integer[] elements = LRTest.this.elements;
            final LeftRight<Set<Integer>> lrSet = LRTest.this.lrSet;

            // ramp-up loop
            while (latch.get() == 0L) {
                Integer e = elements[i++];
                if (i >= elements.length) i = 0;
                lrSet.read(e, (_e, set) -> set.contains(_e));
            }

            // count-ops loop
            long ops = 0L;
            while (latch.get() == 1L) {
                Integer e = elements[i++];
                if (i >= elements.length) i = 0;
                lrSet.read(e, (_e, set) -> set.contains(_e));
                ops++;
            }
            this.ops = ops;

            // ramp-down loop
            while (latch.get() == 2L) {
                Integer e = elements[i++];
                if (i >= elements.length) i = 0;
                lrSet.read(e, (_e, set) -> set.contains(_e));
            }
        }
    }

    class Writer extends Worker {

        private final boolean odd;

        Writer(AtomicInteger latch, boolean odd) {
            super(latch);
            this.odd = odd;
        }

        @Override
        public void run() {
            // decrement and wait for all threads to arrive
            latch.decrementAndGet();
            while (latch.get() > 0L) {}

            // initial indexes into elements array
            int iRemove;
            int iAdd;
            if (odd) { // odd indexes
                iRemove = 1;
                iAdd = 1 + quarter;
            } else { // even indexes
                iRemove = 0;
                iAdd = quarter;
            }

            // cache instance variables in locals
            final Integer[] elements = LRTest.this.elements;
            final LeftRight<Set<Integer>> lrSet = LRTest.this.lrSet;

            // ramp-up loop
            while (latch.get() == 0L) {
                Integer eRemove = elements[iRemove];
                Integer eAdd = elements[iAdd];
                if ((iRemove += 2) >= elements.length)
                    iRemove -= elements.length;
                if ((iAdd += 2) >= elements.length) iAdd -= elements.length;
                lrSet.modify(eRemove, (e, set) -> set.remove(e));
                lrSet.modify(eAdd, (e, set) -> set.add(e));
            }

            // count-ops loop
            long ops = 0L;
            while (latch.get() == 1L) {
                Integer eRemove = elements[iRemove];
                Integer eAdd = elements[iAdd];
                if ((iRemove += 2) >= elements.length)
                    iRemove -= elements.length;
                if ((iAdd += 2) >= elements.length) iAdd -= elements.length;
                lrSet.modify(eRemove, (e, set) -> set.remove(e));
                lrSet.modify(eAdd, (e, set) -> set.add(e));
                ops += 2;
            }
            this.ops = ops;

            // ramp-down loop
            while (latch.get() == 2L) {
                Integer eRemove = elements[iRemove];
                Integer eAdd = elements[iAdd];
                if ((iRemove += 2) >= elements.length)
                    iRemove -= elements.length;
                if ((iAdd += 2) >= elements.length) iAdd -= elements.length;
                lrSet.modify(eRemove, (e, set) -> set.remove(e));
                lrSet.modify(eAdd, (e, set) -> set.add(e));
            }
        }
    }
}
