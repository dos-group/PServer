package de.tuberlin.pserver.dsl.transaction.aggregators;


import com.google.common.base.Preconditions;
import de.tuberlin.pserver.runtime.RuntimeManager;
import de.tuberlin.pserver.runtime.driver.ProgramContext;
import de.tuberlin.pserver.runtime.events.MsgEventHandler;
import de.tuberlin.pserver.runtime.parallel.shared.SharedVar;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Aggregator<T extends Serializable> {

    // TODO: PRESERVE ORDER OF RECEIVED PARTIAL AGGREGATES ? !

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private static final int AGG_NODE_ID = 0;

    static public interface AggregatorFunction<T> {

        public T apply(final List<T> partialAggs);
    }

    // ---------------------------------------------------

    private final ProgramContext pc;

    private final T partialAgg;

    private final SharedVar<T> sharedGlobalAgg;

    private final boolean symmetricAgg;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public Aggregator(final ProgramContext pc, final T partialAgg) throws Exception { this(pc, partialAgg, true); }
    public Aggregator(final ProgramContext pc, final T partialAgg, final boolean symmetricAgg) throws Exception {

        this.pc = Preconditions.checkNotNull(pc);

        this.partialAgg = Preconditions.checkNotNull(partialAgg);

        this.symmetricAgg = symmetricAgg;

        this.sharedGlobalAgg = new SharedVar<>(pc, partialAgg);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public T apply(final AggregatorFunction<T> function) throws Exception {
        return symmetricAgg ? symmetric_apply(function) : asymmetric_apply(function);
    }

    // ---------------------------------------------------
    // Private Methods.
    // ---------------------------------------------------

    private T symmetric_apply(final AggregatorFunction<T> function) throws Exception {

        pc.runtimeContext.runtimeManager.send(aggPushUID(), partialAgg);

        final int n = pc.nodeDOP - 1;

        final List<T> partialAggs = new ArrayList<>();

        for (int i = 0; i < n + 1; ++i)
            partialAggs.add(null);

        partialAggs.set(pc.runtimeContext.nodeID, partialAgg);

        pc.runtimeContext.runtimeManager.receive(RuntimeManager.ReceiveType.SYNC, n, aggPushUID(), new MsgEventHandler() {

            @Override
            @SuppressWarnings("unchecked")
            public void handleMsg(int srcNodeID, Object value) {
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

            pc.runtimeContext.runtimeManager.receive(RuntimeManager.ReceiveType.SYNC, n, aggPushUID(), new MsgEventHandler() {

                @Override
                @SuppressWarnings("unchecked")
                public void handleMsg(int srcNodeID, Object value) {
                    partialAggs.add((T) value);
                }
            });

            final T agg = function.apply(partialAggs);

            pc.runtimeContext.runtimeManager.send(aggPushUID(), agg);

            sharedGlobalAgg.set(agg);
        }


        if (pc.node(AGG_NODE_ID + 1, pc.nodeDOP - 1)) {

            pc.runtimeContext.runtimeManager.send(aggPushUID(), partialAgg, new int[]{AGG_NODE_ID});

            pc.runtimeContext.runtimeManager.receive(RuntimeManager.ReceiveType.SYNC, 1, aggPushUID(), new MsgEventHandler() {
                @Override
                @SuppressWarnings("unchecked")
                public void handleMsg(int srcNodeID, Object value) {
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
