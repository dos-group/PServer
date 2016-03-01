package de.tuberlin.pserver.node;

import de.tuberlin.pserver.runtime.core.config.ConfigLoader;

public final class PServerMain {

    // ---------------------------------------------------
    // PServer Entry Point.
    // ---------------------------------------------------

    public static void main(String[] args) {

        if (args == null || args.length != 1)
            throw new IllegalStateException();

        PServerNodeFactory.createNode(ConfigLoader.loadResource(args[0]));
    }
}
