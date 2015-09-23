package de.tuberlin.pserver.dsl.dataflow.aggregators;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.dataflow.shared.SharedVar;
import de.tuberlin.pserver.runtime.DataManager;
import de.tuberlin.pserver.runtime.ExecutionManager;
import de.tuberlin.pserver.runtime.SlotContext;

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

    private final SlotContext sc;

    private final T partialAgg;

    private final SharedVar<T> sharedGlobalAgg;

    private final boolean symmetricAgg;

    // ---------------------------------------------------

    public Aggregator(final SlotContext sc, final T partialAgg) throws Exception { this(sc, partialAgg, true); }
    public Aggregator(final SlotContext sc, final T partialAgg, final boolean symmetricAgg) throws Exception {

        this.sc = Preconditions.checkNotNull(sc);

        this.partialAgg = Preconditions.checkNotNull(partialAgg);

        this.symmetricAgg = symmetricAgg;

        this.sharedGlobalAgg = new SharedVar<>(sc, partialAgg);
    }

    // ---------------------------------------------------

    public T apply(final AggregatorFunction<T> function) throws Exception {
        return symmetricAgg ? symmetric_apply(function) : asymmetric_apply(function);
    }

    // ---------------------------------------------------

    private T symmetric_apply(final AggregatorFunction<T> function) throws Exception {

        sc.runtimeContext.dataManager.pushTo(aggPushUID(), partialAgg);

        final int n = sc.programContext.nodeDOP - 1;

        final List<T> partialAggs = new ArrayList<>();

        for (int i = 0; i < n + 1; ++i)
            partialAggs.add(null);

        partialAggs.set(sc.runtimeContext.nodeID, partialAgg);

        sc.runtimeContext.dataManager.awaitEvent(ExecutionManager.CallType.SYNC, n, aggPushUID(), new DataManager.DataEventHandler() {

            @Override
            @SuppressWarnings("unchecked")
            public void handleDataEvent(int srcNodeID, Object value) {
                partialAggs.set(srcNodeID, (T)value);
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

        if (sc.node(AGG_NODE_ID)) {

            final int n = sc.programContext.nodeDOP - 1;

            final List<T> partialAggs = new ArrayList<>();

            partialAggs.add(partialAgg);

            sc.runtimeContext.dataManager.awaitEvent(ExecutionManager.CallType.SYNC, n, aggPushUID(), new DataManager.DataEventHandler() {

                @Override
                @SuppressWarnings("unchecked")
                public void handleDataEvent(int srcNodeID, Object value) {
                    partialAggs.add((T) value);
                }
            });

            final T agg = function.apply(partialAggs);

            sc.runtimeContext.dataManager.pushTo(aggPushUID(), agg);

            sharedGlobalAgg.set(agg);
        }


        if (sc.node(AGG_NODE_ID + 1, sc.programContext.nodeDOP - 1)) {

            sc.runtimeContext.dataManager.pushTo(aggPushUID(), partialAgg, new int[]{AGG_NODE_ID});

            sc.runtimeContext.dataManager.awaitEvent(ExecutionManager.CallType.SYNC, 1, aggPushUID(), new DataManager.DataEventHandler() {

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
