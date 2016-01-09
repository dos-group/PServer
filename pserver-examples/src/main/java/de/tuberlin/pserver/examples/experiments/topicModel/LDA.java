package de.tuberlin.pserver.examples.experiments.topicModel;

import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.Scope;
import de.tuberlin.pserver.dsl.transaction.TransactionDefinition;
import de.tuberlin.pserver.dsl.transaction.annotations.Transaction;
import de.tuberlin.pserver.dsl.transaction.phases.Update;
import de.tuberlin.pserver.dsl.transaction.properties.TransactionType;
import de.tuberlin.pserver.dsl.unit.UnitMng;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.matrix.ElementType;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.math.matrix.Matrix64F;
import de.tuberlin.pserver.runtime.parallel.Parallel;
import de.tuberlin.pserver.runtime.state.MatrixBuilder;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.List;
import java.util.Random;


public class LDA extends Program{

    // ---------------------------------------------------
    // Parameters
    // ---------------------------------------------------

    // Configuration
    private static final String NUM_NODES = "1";

    // Input
    private static final String DOC_TERM_PATH = "/Users/Chris/Downloads/reuters_doc_term.csv";
    private static final int N_DOCUMENTS = 395;
    private static final int N_VOCABULARY = 4258;

    // Hyperparameter
    private static final int N_TOPICS = 20;
    private static final int N_ITER = 1000;
    private static final double ALPHA = 0.1;
    private static final double BETA = 0.1;


    // ---------------------------------------------------
    // State
    // ---------------------------------------------------

    // D is a document-term matrix, representing the corpus
    @State(scope = Scope.PARTITIONED, rows = N_DOCUMENTS, cols = N_VOCABULARY, path = DOC_TERM_PATH)
    public Matrix64F D;

    // N_kw keeps track of how instances of a vocabulary word are assigned to topic k
    @State(scope = Scope.REPLICATED, rows = N_TOPICS, cols = N_VOCABULARY)
    public Matrix64F N_kw;

    // N_k keeps track of how many words w are assigned to topic k
    @State(scope = Scope.REPLICATED, rows = N_TOPICS, cols = 1)
    public Matrix64F N_k;

    // theta is the distribution over topics for each document
    //@State(scope = Scope.PARTITIONED, rows = N_DOCUMENTS, cols = N_TOPICS)
    //public Matrix64F theta;

    // phi is the distribution over words for each topic
    //@State(scope = Scope.PARTITIONED, rows = N_VOCABULARY, cols = N_TOPICS)
    //public Matrix64F phi;

    // N_dk keeps track of how many words in document d belong to topic k
    public Matrix64F N_dk;

    // Z is the topic assignment of each word w belonging to the local partition of the corpus D
    public Matrix32F Z;


    // ---------------------------------------------------
    // Transactions.
    // ---------------------------------------------------

    @Transaction(state = "N_kw, N_k", type = TransactionType.PULL)
    public final TransactionDefinition sync = new TransactionDefinition(

            // TODO: this must be a delta update of (latest local state) - (state at previous sync)

            (Update<Matrix64F>) (remoteUpdates, localState) -> {
                for (final Matrix64F update : remoteUpdates)
                    Parallel.For(update, (i, j, v) -> localState.set(i, j, localState.get(i, j) + update.get(i, j)));
            }
    );


    // ---------------------------------------------------
    // Units
    // ---------------------------------------------------

    @Unit
    public void main(Lifecycle lifecycle) {
        lifecycle.preProcess(() -> {
            // number of words in the local partition of the corpus D
            int nWords = D.sum().intValue();

            Z = new MatrixBuilder()
                    .dimension(nWords, 1)
                    .elementType(ElementType.FLOAT_MATRIX)
                    .build();

            N_dk = new MatrixBuilder()
                    .dimension(N_DOCUMENTS, N_TOPICS)
                    .elementType(ElementType.DOUBLE_MATRIX)
                    .build();

            Random rand = new Random();

            // randomly initialize the topic z of each word w
            //Z.applyOnElements((e) -> (float)rand.nextInt(N_TOPICS), Z);
            Parallel.For(Z, (i) -> {
                Z.set(i, 0, (float)rand.nextInt(N_TOPICS));
            });

            // initialize N_dk, N_wk and N_k based on the initialization of Z
            long zIndex = 0;

            for (int d = 0; d < D.rows(); d++) {
                for (int w = 0; w < D.cols(); w++) {
                    int wCount = D.get(d, w).intValue();
                    if (wCount != 0) {
                        for (int i = 0; i < wCount; i++) {
                            int topic = Z.get(zIndex).intValue();

                            N_dk.set(d, topic, N_dk.get(d, topic) + 1);
                            N_kw.set(topic, w, N_kw.get(topic, w) + 1);
                            N_k.set(topic, 0, N_k.get(topic) + 1);

                            zIndex++;
                        }
                    }
                }
            }
        }).process(() -> {

            UnitMng.loop(N_ITER, (iter) -> {
                // TODO: find a better solution for intra-node parallelism
                long zIndex = 0;
                Random rand = new Random();

                for (int d = 0; d < N_DOCUMENTS; d++) {
                    for (int w = 0; w < N_VOCABULARY; w++) {
                        int wCount = D.get(d, w).intValue();
                        if (wCount != 0) {
                            for (int i = 0; i < wCount; i++) {
                                int topic = Z.get(zIndex).intValue();

                                N_dk.set(d, topic, N_dk.get(d, topic) - 1);
                                N_kw.set(topic, w, N_kw.get(topic, w) - 1);
                                N_k.set(topic, 0, N_k.get(topic) - 1);

                                Matrix64F pZ = new MatrixBuilder().
                                        dimension(N_TOPICS, 1)
                                        .elementType(ElementType.DOUBLE_MATRIX)
                                        .build();

                                double pZsum = 0.0;

                                for (int k = 0; k < N_TOPICS; k++) {
                                    double p_zk = (N_dk.get(d, k) + ALPHA) * (N_kw.get(k, w) + BETA) / (N_k.get(k) + BETA * N_VOCABULARY);
                                    pZsum += p_zk;
                                    pZ.set(k, 0, pZsum);
                                }

                                pZ.scale(1. / pZsum, pZ);

                                double u = rand.nextDouble();

                                int newTopic = 0;

                                int k = 0;
                                boolean sampled = false;
                                while (k < N_TOPICS && !sampled) {
                                    if (u < pZ.get(k)) {
                                        newTopic = k;
                                        sampled = true;
                                    }
                                    k++;
                                }

                                Z.set(zIndex, 0, (float) newTopic);

                                N_dk.set(d, newTopic, N_dk.get(d, newTopic) + 1);
                                N_kw.set(newTopic, w, N_kw.get(newTopic, w) + 1);
                                N_k.set(newTopic, 0, N_k.get(newTopic) + 1);

                                zIndex++;
                            }
                        }
                    }
                }
            });

            // TODO: global sync of N_k and N_wk

        }).postProcess(() -> {

            // TODO: this must be global
            Matrix64F theta = new MatrixBuilder()
                    .dimension(N_DOCUMENTS, N_TOPICS)
                    .elementType(ElementType.DOUBLE_MATRIX)
                    .build();

            Matrix64F phi = new MatrixBuilder()
                    .dimension(N_TOPICS, N_VOCABULARY)
                    .elementType(ElementType.DOUBLE_MATRIX)
                    .build();

            for (int k = 0; k < N_TOPICS; k++) {
                for (int w = 0; w < N_VOCABULARY; w++) {
                    phi.set(k, w, (N_kw.get(k, w) + BETA) / (N_k.get(k) + N_VOCABULARY * BETA));
                }
            }

            for (int d = 0; d < N_DOCUMENTS; d++) {
                int nWords = D.getRow(d).sum().intValue();
                for (int k = 0; k < N_TOPICS; k++) {
                    theta.set(d, k, (N_dk.get(d, k) + ALPHA) / (nWords + N_TOPICS * ALPHA));
                }
            }

            // TODO: check why this fails in case theta/phi is partitioned
            result(theta, phi);
        });
        }

                // ---------------------------------------------------
                // EntryImpl Point
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

        Matrix64F theta = (Matrix64F) result.get(0).get(0);
        Matrix64F phi = (Matrix64F) result.get(0).get(1);

        try (PrintWriter writer = new PrintWriter("/Users/Chris/Downloads/theta.csv", "UTF-8")) {
            for (int i = 0; i < theta.rows(); ++i) {
                for (int j = 0; j < theta.cols(); ++j) {
                    writer.print(theta.get(i, j));
                    if (j+1 < theta.cols()) {
                        writer.print(",");
                    }
                }
                writer.println();
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }

        try (PrintWriter writer = new PrintWriter("/Users/Chris/Downloads/phi.csv", "UTF-8")) {
            for (int i = 0; i < phi.rows(); ++i) {
                for (int j = 0; j < phi.cols(); ++j) {
                    writer.print(phi.get(i, j));
                    if (j+1 < phi.cols()) {
                        writer.print(",");
                    }
                }
                writer.println();
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }
}
