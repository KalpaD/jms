package com.kalpada.jms;

/**
 * Created by Kalpa on 28/8/16.
 */

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.util.concurrent.TimeUnit.*;

class PollController {

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public void pollForever() {

        final Runnable beeper = new Runnable() {
            public void run() {
                Consumer consumer = new Consumer();
                consumer.handleMessage();
            }
        };

        final ScheduledFuture<?> beeperHandle = scheduler.scheduleAtFixedRate(beeper, 5, 5, SECONDS);

        /*scheduler.schedule( new Runnable() {
            public void run() {
                beeperHandle.cancel(true);
            }
        }, 10 , SECONDS);*/
    }

    public static void main(String [] args) {
        PollController bc = new PollController();
        bc.pollForever();
    }
}

