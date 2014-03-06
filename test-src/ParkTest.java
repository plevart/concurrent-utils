/*
 * Copyright (C) 2014 Peter Levart
 *
 * This work is licensed under a Creative Commons Attribution 3.0 Unported License:
 * http://creativecommons.org/licenses/by/3.0/
 */

import java.util.concurrent.locks.LockSupport;

/**
 * @author peter
 */
public class ParkTest {
    public static void main(String[] args) throws Exception {
        Thread t = new Thread() {
            @Override
            public void run() {
                System.out.println("Parking...");
                LockSupport.park();
                System.out.println("Wake-up. Is interrupted? " + Thread.interrupted());
                System.out.println("Parking again...");
                LockSupport.park();
                System.out.println("Wake-up. Is interrupted? " + Thread.interrupted());
                System.out.println("Finished.");
            }
        };

        t.start();
        Thread.sleep(3000L);
        t.interrupt();
        //LockSupport.unpark(t);
        Thread.sleep(3000L);
        LockSupport.unpark(t);

        t.join();

    }
}
