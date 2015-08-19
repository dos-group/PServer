package de.tuberlin.pserver.dsl.dataflow;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.SharedObject;
import de.tuberlin.pserver.runtime.SlotContext;
import de.tuberlin.pserver.runtime.delta.LZ4MatrixDeltaExtractor;
import de.tuberlin.pserver.runtime.delta.MatrixDeltaFilter;
import de.tuberlin.pserver.runtime.dht.DHTKey;


public final class DataFlow {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private SlotContext slotContext;

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public DataFlow(final SlotContext slotContext) {
        this.slotContext = Preconditions.checkNotNull(slotContext);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public <T extends SharedObject> DHTKey put(final String name, final T obj) {
        return slotContext.programContext.runtimeContext.dataManager.putObject(name, obj);
    }

    public <T extends SharedObject> T get(final String name) {
        return slotContext.programContext.runtimeContext.dataManager.getObject(name);
    }

    // ---------------------------------------------------

    public void computeDelta(final String name) throws Exception {
        final LZ4MatrixDeltaExtractor deltaExtractor =
                (LZ4MatrixDeltaExtractor)slotContext.programContext.get(name + "-Delta-Extractor");
        deltaExtractor.extractDeltas(slotContext);
    }
}
