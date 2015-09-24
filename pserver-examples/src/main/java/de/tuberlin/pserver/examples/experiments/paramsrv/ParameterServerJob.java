package de.tuberlin.pserver.examples.experiments.paramsrv;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.controlflow.annotations.Unit;
import de.tuberlin.pserver.dsl.controlflow.loop.Loop;
import de.tuberlin.pserver.dsl.controlflow.program.Lifecycle;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.GlobalScope;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.tuples.Tuple2;
import de.tuberlin.pserver.math.tuples.Tuple3;
import de.tuberlin.pserver.mcruntime.Parallel;
import de.tuberlin.pserver.runtime.DataManager;
import de.tuberlin.pserver.runtime.Program;

import java.util.ArrayList;
import java.util.List;

public class ParameterServerJob extends Program {

    @State(globalScope = GlobalScope.SINGLETON, format = Format.SPARSE_FORMAT, rows = 1, cols = 1000, at = "0")
    public Matrix parameters;

    @State(globalScope = GlobalScope.PARTITIONED, format = Format.SPARSE_FORMAT, rows = 100, cols = 100, at = "1 - 3")
    public Matrix input;


    @Unit(at = "0")
    public void serverMain(final Lifecycle lifecycle) {
        final DataManager dataManager = programContext.runtimeContext.dataManager;
        lifecycle.process(() -> {
            CF.loop().sync(Loop.GLOBAL).exe(15, (epoch) -> {

                dataManager.receive(3, "parameterPull-Request", (srcNodeID, value) -> {

                    @SuppressWarnings("unchecked") final List<Tuple2<Integer, Integer>> requestedParams = (List<Tuple2<Integer, Integer>>) value;

                    final List<Tuple3<Integer, Integer, Double>> responseParams = new ArrayList<>();

                    for (final Tuple2<Integer, Integer> param : requestedParams) {

                        responseParams.add(new Tuple3<>(param._1, param._2, parameters.get(param._1, param._2)));
                    }

                    dataManager.pushTo("parameterPull-Response", responseParams, new int[] { srcNodeID });
                });

                dataManager.receive(3, "gradientPush", (srcNodeID, value) -> {

                });
            });
        });
    }


    @Unit(at = "1 - 3")
    public void workerMain(final Lifecycle lifecycle) {
        final DataManager dataManager = programContext.runtimeContext.dataManager;
        lifecycle.process(() -> {

            CF.loop().sync(Loop.GLOBAL).exe(15, (epoch) -> {

                Parallel.For(input, (i, j, value) -> {

                    final List<Tuple2<Integer, Integer>> requestedParams = new ArrayList<>();

                    dataManager.pushTo("parameterPull-Request", requestedParams, new int[]{0});

                    final List<Tuple3<Integer, Integer, Double>> params = new ArrayList<>();

                    dataManager.receive(1, "parameterPull-Response", (srcNodeID, response) -> {

                        params.addAll((List)response);
                    });

                    params.clear();

                    dataManager.pushTo("gradientPush", new Tuple3<>(0, 0, 1.0), new int[] {0});
                });
            });
        });
    }


    public static void main(final String[] args) {
        System.setProperty("simulation.numNodes", "4");
        PServerExecutor.LOCAL
                .run(ParameterServerJob.class)
                .done();
    }
}
