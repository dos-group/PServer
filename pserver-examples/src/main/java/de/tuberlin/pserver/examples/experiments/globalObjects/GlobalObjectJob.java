package de.tuberlin.pserver.examples.experiments.globalObjects;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.GlobalObject;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.types.matrix.annotations.Matrix;
import de.tuberlin.pserver.types.matrix.implementation.Matrix32F;
import de.tuberlin.pserver.types.matrix.implementation.matrix32f.dense.DenseMatrix32F;
import de.tuberlin.pserver.types.typeinfo.annotations.GlobalAccess;
import de.tuberlin.pserver.types.typeinfo.properties.DistScheme;

import java.util.ArrayList;
import java.util.List;

public class GlobalObjectJob extends Program {

    @GlobalAccess
    @Matrix(scheme = DistScheme.REPLICATED, at = "0", rows = 1, cols = 10000)
    public Matrix32F parameters;

    @GlobalObject(at = "0", impl = ArrayList.class)
    public List<String> globalList;

    @Unit(at = "1")
    public void worker(final Lifecycle lifecycle) {

        lifecycle.process(()-> {

            float sum = 0;
            for (int i = 0; i < 10000; ++i) {
                parameters.set(0, i, 16.71985f + i);
                sum += parameters.get(0, i);
            }

            System.out.println("sum: " + sum);
            for (int i = 0; i < 10000; ++i) {
                globalList.add("Test " + i);
            }
            for (int i = 0; i < 10000; ++i) {
                System.out.println(globalList.get(i));
            }

        });
    }

    public static void main(final String[] args) {

        System.setProperty("simulation.numNodes", "2");
        PServerExecutor.LOCAL
                .run(GlobalObjectJob.class)
                .done();
    }
}
