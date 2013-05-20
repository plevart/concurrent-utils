/*
 * Written by Peter Levart <peter.levart@gmail.com>
 * and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */
package si.pele.concurrent;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * @author peter
 */
public class Container {

    // state0 (2 bits per lazy field, good for 1st 16 lazy fields - next 16 fields will have state1, etc...)
    private volatile int state0;

    // CAS support for state0
    private static final AtomicIntegerFieldUpdater<Container> state0Updater
        = AtomicIntegerFieldUpdater.newUpdater(Container.class, "state0");

    // the lazy val field
    private int lazyVal;

    // lock for lazyVal initialization
    private static final LazyLock<Container> lazyValLock
        = new LazyLock<Container>(state0Updater, 0);

    private int computeLazyVal() {
        // this is the expression used to initialize lazy val
        System.out.println(Thread.currentThread().getName() + ": computeLazyVal called");
        try {
            Thread.sleep(50L);
        } catch (InterruptedException e) {}
        return 123;
    }

    public int getLazyVal() {
        if (lazyValLock.lock(this)) {
            try {
                return lazyVal = computeLazyVal();
            } finally {
                lazyValLock.unlock(this);
            }
        } else {
            return lazyVal;
        }
    }


    // test
    public static void main(String[] args) throws Exception {

        class Task implements Runnable {
            final Container c;

            Task(Container c) {
                this.c = c;
            }

            @Override
            public void run() {
                System.out.println(Thread.currentThread().getName() + ": c.lazyVal = " + c.getLazyVal());
            }
        }

        {
            System.out.println("\nSingle threaded initialization...");
            Container c = new Container();
            new Task(c).run();
        }

        {
            System.out.println("\nDouble threaded initialization...");
            Container c = new Container();
            Runnable r = new Task(c);
            Thread t1 = new Thread(r, "t1");
            Thread t2 = new Thread(r, "t2");
            t1.start();
            t2.start();
            t1.join();
            t2.join();
            // final non-competing get
            r.run();
        }

        {
            System.out.println("\nTriple threaded initialization...");
            Container c = new Container();
            Runnable r = new Task(c);
            Thread t1 = new Thread(r, "t1");
            Thread t2 = new Thread(r, "t2");
            Thread t3 = new Thread(r, "t3");
            t3.start(); // 1st start t3 this time
            t1.start();
            t2.start();
            t1.join();
            t2.join();
            t3.join();
            // final non-competing get
            r.run();
        }
    }
}
