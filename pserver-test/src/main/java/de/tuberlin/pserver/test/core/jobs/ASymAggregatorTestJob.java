package de.tuberlin.pserver.test.core.jobs;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.dsl.Aggregator;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.runtime.MLProgram;
import org.apache.commons.lang3.tuple.Pair;


public class ASymAggregatorTestJob extends MLProgram {

    @Override
    public void define(final Program program) {

        program.process(() -> {

            final int partialAgg = slotContext.programContext.runtimeContext.nodeID * 1000;

            final int globalAgg = new Aggregator<>(slotContext, partialAgg, false)
                    .apply(pa -> pa.stream().mapToInt(Integer::intValue).sum());

            Preconditions.checkState(globalAgg == 6000);
        });
    }
}
