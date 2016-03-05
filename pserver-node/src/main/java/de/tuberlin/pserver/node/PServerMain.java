package de.tuberlin.pserver.node;

import de.tuberlin.pserver.runtime.core.config.ConfigLoader;
import de.tuberlin.pserver.runtime.core.diagnostics.MemoryTracer;

public final class PServerMain {

    // ---------------------------------------------------
    // PServer Entry Point.
    // ---------------------------------------------------

    public static void main(String[] args) {

        if (args == null || args.length != 1)
            throw new IllegalStateException();

        System.out.println(MemoryTracer.getTrace("Initial"));

        PServerNodeFactory.createNode(ConfigLoader.loadResource(args[0]));
    }
}
