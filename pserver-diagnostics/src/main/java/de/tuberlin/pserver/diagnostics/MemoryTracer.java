package de.tuberlin.pserver.diagnostics;

import com.google.gson.Gson;
import de.tuberlin.pserver.commons.config.Config;

public final class MemoryTracer {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    public static final String CONFIG_TRACE_MEMORY = "global.debug.traceMemory";

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static Gson gson = new Gson();
    private static Config config;

    // ---------------------------------------------------

    private final long currentTime;
    private final long totalMemory;
    private final long freeMemory;
    private final long usedMemory;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    private MemoryTracer() {
        this.currentTime = System.currentTimeMillis();
        this.totalMemory = Runtime.getRuntime().totalMemory();
        this.freeMemory = Runtime.getRuntime().freeMemory();
        this.usedMemory = totalMemory - freeMemory;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public static void setConfig(Config config) { MemoryTracer.config = config; }

    public static String getTrace(String tracePointName) {
        return "\n memory-trace@[" + Thread.currentThread().getStackTrace()[1].getMethodName()
                + " | "+ tracePointName +"]:\n" + new MemoryTracer().toString() + "\n";
    }

    public static void printTrace(String tracePointName) {
        if (config.getBoolean(CONFIG_TRACE_MEMORY))
            System.out.println(getTrace(tracePointName));
    }

    @Override
    public String toString() { return gson.toJson(this); }
}
