package de.tuberlin.pserver.runtime.core.diagnostics;

import com.google.gson.Gson;
import de.tuberlin.pserver.runtime.state.matrix.MatrixLoader;

public final class MemoryTracer {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static Gson gson = new Gson();

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

    @Override
    public String toString() {
        return gson.toJson(this);
    }

    public static String getTrace(String tracePointName) {
        return "\n memory-trace@[" + Thread.currentThread().getStackTrace()[1].getMethodName()
                + " | "+ tracePointName +"]:\n" + new MemoryTracer().toString() + "\n";
    }
}
