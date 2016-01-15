package de.tuberlin.pserver.examples.experiments.paramsrv;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.matrix.Format;
import de.tuberlin.pserver.math.matrix.Matrix32F;

public class ParameterServerJob extends Program {

    @State(scope = Scope.REPLICATED, format = Format.SPARSE_FORMAT, rows = 1, cols = 1000, at = "0")
    public Matrix32F parameters;


    @Unit(at = "1")
    public void worker(final Lifecycle lifecycle) {

        try {

            for (int i = 0; i < 30; ++i) {

                parameters.set(0, i, 16.71985f + i);

                Thread.sleep(3000);

                float f = parameters.get(0, i);

                System.out.println("YEAH IT WORKS -> " + f);
            }

        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static void main(final String[] args) {

        System.setProperty("simulation.numNodes", "2");
        PServerExecutor.LOCAL
                .run(ParameterServerJob.class)
                .done();
    }
}
