package de.tuberlin.pserver.examples.experiments.topicModel;

import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.Matrix32F;

import java.io.Serializable;
import java.util.List;


public class LDA extends Program{

    // ---------------------------------------------------
    // Parameters
    // ---------------------------------------------------

    // Configuration
    private static final String NUM_NODES = "2";

    // Input
    private static final String DOC_TERM_PATH = "";
    private static final int N_DOCUMENTS = 20;
    private static final int N_VOCABULARY = 20;

    // Hyperparameter
    private static final int N_TOPICS = 20;
    private static final int N_ITER = 1;
    private static final double ALPHA = 0.1;
    private static final double BETA = 0.1;


    // ---------------------------------------------------
    // State
    // ---------------------------------------------------

    @State(scope = Scope.PARTITIONED, rows = N_DOCUMENTS, cols = N_VOCABULARY, path = DOC_TERM_PATH)
    public Matrix32F X;

    @State(scope = Scope.PARTITIONED, rows = N_DOCUMENTS, cols = N_TOPICS)
    public Matrix32F N_dk;

    @State(scope = Scope.REPLICATED, rows = N_VOCABULARY, cols = N_TOPICS)
    public Matrix32F N_wk;

    @State(scope = Scope.REPLICATED, rows = 1, cols = N_TOPICS)
    public Matrix32F N_k;

    @State(scope = Scope.PARTITIONED, rows = 1, cols = N_TOPICS)
    public Matrix32F Z;


    // ---------------------------------------------------
    // Units
    // ---------------------------------------------------

    @Unit
    public void unit(Lifecycle lifecycle) {
        lifecycle.preProcess(() -> {

        }).process(() -> {

        }).postProcess(() -> {

        });
    }

    // ---------------------------------------------------
    // Entry Point
    // ---------------------------------------------------

    public static void main(final String[] args) { local(); }

    // ---------------------------------------------------

    private static void local() {
        System.setProperty("simulation.numNodes", NUM_NODES);

        final List<List<Serializable>> result = Lists.newArrayList();

        PServerExecutor.LOCAL
                .run(LDA.class)
                .results(result)
                .done();

        Matrix model = (Matrix) result.get(0).get(0);
        System.out.println(model);
    }
}
