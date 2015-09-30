package de.tuberlin.pserver.dsl.transaction.aggregators;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.compiler.ProgramContext;
import de.tuberlin.pserver.runtime.DataManager;
import de.tuberlin.pserver.runtime.mcruntime.shared.SharedVar;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Aggregator<T extends Serializable> {

    // TODO: PRESERVE ORDER OF RECEIVED PARTIAL AGGREGATES ? !

    private static final int AGG_NODE_ID = 0;

    // ---------------------------------------------------

    static public interface AggregatorFunction<T> {

        public T apply(final List<T> partialAggs);
    }

    // ---------------------------------------------------

    private final ProgramContext pc;

    private final T partialAgg;

    private final SharedVar<T> sharedGlobalAgg;

    private final boolean symmetricAgg;

    // ---------------------------------------------------

    public Aggregator(final ProgramContext pc, final T partialAgg) throws Exception { this(pc, partialAgg, true); }
    public Aggregator(final ProgramContext pc, final T partialAgg, final boolean symmetricAgg) throws Exception {

        this.pc = Preconditions.checkNotNull(pc);

        this.partialAgg = Preconditions.checkNotNull(partialAgg);

        this.symmetricAgg = symmetricAgg;

        this.sharedGlobalAgg = new SharedVar<>(pc, partialAgg);
    }

    // ---------------------------------------------------

    public T apply(final AggregatorFunction<T> function) throws Exception {
        return symmetricAgg ? symmetric_apply(function) : asymmetric_apply(function);
    }

    // ---------------------------------------------------

    private T symmetric_apply(final AggregatorFunction<T> function) throws Exception {

        pc.runtimeContext.dataManager.pushTo(aggPushUID(), partialAgg);

        final int n = pc.nodeDOP - 1;

        final List<T> partialAggs = new ArrayList<>();

        for (int i = 0; i < n + 1; ++i)
            partialAggs.add(null);

        partialAggs.set(pc.runtimeContext.nodeID, partialAgg);

        pc.runtimeContext.dataManager.receive(DataManager.CallType.SYNC, n, aggPushUID(), new DataManager.DataEventHandler() {

            @Override
            @SuppressWarnings("unchecked")
            public void handleDataEvent(int srcNodeID, Object value) {
                partialAggs.set(srcNodeID, (T) value);
            }
        });

        final T agg = function.apply(partialAggs);

        sharedGlobalAgg.set(agg);

        final T resultAgg = sharedGlobalAgg.get();

        sharedGlobalAgg.done();

        return resultAgg;
    }

    // ---------------------------------------------------

    private T asymmetric_apply(final AggregatorFunction<T> function) throws Exception {

        // -- master node --

        if (pc.node(AGG_NODE_ID)) {

            final int n = pc.nodeDOP - 1;

            final List<T> partialAggs = new ArrayList<>();

            partialAggs.add(partialAgg);

            pc.runtimeContext.dataManager.receive(DataManager.CallType.SYNC, n, aggPushUID(), new DataManager.DataEventHandler() {

                @Override
                @SuppressWarnings("unchecked")
                public void handleDataEvent(int srcNodeID, Object value) {
                    partialAggs.add((T) value);
                }
            });

            final T agg = function.apply(partialAggs);

            pc.runtimeContext.dataManager.pushTo(aggPushUID(), agg);

            sharedGlobalAgg.set(agg);
        }


        if (pc.node(AGG_NODE_ID + 1, pc.nodeDOP - 1)) {

            pc.runtimeContext.dataManager.pushTo(aggPushUID(), partialAgg, new int[]{AGG_NODE_ID});

            pc.runtimeContext.dataManager.receive(DataManager.CallType.SYNC, 1, aggPushUID(), new DataManager.DataEventHandler() {

                @Override
                @SuppressWarnings("unchecked")
                public void handleDataEvent(int srcNodeID, Object value) {
                    sharedGlobalAgg.set((T) value);
                }
            });
        }

        final T resultAgg = sharedGlobalAgg.get();

        sharedGlobalAgg.done();

        return resultAgg;
    }

    // ---------------------------------------------------

    private String aggPushUID() { return "agg"; }
}
