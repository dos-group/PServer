package de.tuberlin.pserver.node;

import de.tuberlin.pserver.diagnostics.MemoryTracer;
import de.tuberlin.pserver.commons.config.Config;
import de.tuberlin.pserver.commons.config.ConfigLoader;

public final class PServerMain {

    // ---------------------------------------------------
    // PServer Entry Point.
    // ---------------------------------------------------

    public static void main(String[] args) {

        if (args == null || args.length != 1)
            throw new IllegalStateException();

        Config config = ConfigLoader.loadResource(args[0]);
        MemoryTracer.setConfig(config);
        MemoryTracer.printTrace("Initial");
        PServerNodeFactory.createNode(config);
    }
}
