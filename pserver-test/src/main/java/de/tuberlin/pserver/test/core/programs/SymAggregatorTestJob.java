package de.tuberlin.pserver.test.core.programs;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.Aggregator;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.runtime.MLProgram;


public class SymAggregatorTestJob extends MLProgram {

    @Override
    public void define(final Program program) {

        program.process(() -> {

            final int partialAgg = slotContext.programContext.runtimeContext.nodeID * 1000;

            final int globalAgg = new Aggregator<>(slotContext, partialAgg)
                    .apply(pa -> pa.stream().mapToInt(Integer::intValue).sum());

            Preconditions.checkState(globalAgg == 6000);
        });
    }
}
