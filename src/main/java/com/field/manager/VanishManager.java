package com.field.manager;

import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

public class VanishManager {

    private final Logger logger;
    private final AtomicBoolean vanished = new AtomicBoolean(false);

    public VanishManager(Logger logger) {
        this.logger = logger;
    }

    public boolean isVanished() {
        return vanished.get();
    }

    public void setVanished(boolean state) {
        vanished.set(state);
    }

    public boolean toggle() {
        boolean newState;
        do {
            boolean current = vanished.get();
            newState = !current;
        } while (!vanished.compareAndSet(!newState, newState));
        return newState;
    }
}