package de.tuberlin.pserver.test.core.programs;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.controlflow.annotations.Unit;
import de.tuberlin.pserver.dsl.controlflow.program.Lifecycle;
import de.tuberlin.pserver.dsl.dataflow.aggregators.Aggregator;
import de.tuberlin.pserver.runtime.Program;


public class ASymAggregatorTestJob extends Program {

    @Unit
    public void main(final Lifecycle lifecycle) {

        lifecycle.process(() -> {

            final int partialAgg = programContext.runtimeContext.nodeID * 1000;

            final int globalAgg = new Aggregator<>(programContext, partialAgg, false)
                    .apply(pa -> pa.stream().mapToInt(Integer::intValue).sum());

            Preconditions.checkState(globalAgg == 6000);
        });
    }
}
