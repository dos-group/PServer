package de.tuberlin.pserver.mcruntime;


public abstract class Program implements Runnable {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static MCRuntime mcRuntime;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public Program() {

        if (mcRuntime == null)
            throw new IllegalStateException();
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public static void setMCRuntime(final MCRuntime mcRuntime) { Program.mcRuntime = mcRuntime; }

    public static int slotID() { return mcRuntime.currentSlot().slotID; }

    public static void parallel(final int dop, final ParallelBody parallelBody) throws Exception {

        final WorkerSlot ws = mcRuntime.currentSlot();

        if (ws.slotID == 0) {

            if (ws.empty()) {

                if (dop > 1) {

                    executeByWorkerSlots(dop, parallelBody);

                } else {

                    ws.run(dop, parallelBody);
                }

            } else {

                ws.run(dop, parallelBody);
            }

        } else {

            ws.run(dop, parallelBody);
        }
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private static void executeByWorkerSlots(final int dop, final ParallelBody parallelBody) throws Exception {

        for (int i = dop - 1; i >= 0; --i) {

            mcRuntime.getWorkerSlots()[i].run(dop, parallelBody);
        }
    }
}
