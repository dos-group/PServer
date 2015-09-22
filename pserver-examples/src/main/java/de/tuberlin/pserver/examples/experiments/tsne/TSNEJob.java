package de.tuberlin.pserver.examples.experiments.tsne;

import com.google.common.collect.Lists;
import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.dsl.controlflow.annotations.Unit;
import de.tuberlin.pserver.dsl.controlflow.program.Program;
import de.tuberlin.pserver.dsl.state.annotations.State;
import de.tuberlin.pserver.dsl.state.properties.GlobalScope;
import de.tuberlin.pserver.dsl.state.properties.RemoteUpdate;
import de.tuberlin.pserver.math.Format;
import de.tuberlin.pserver.math.matrix.Matrix;
import de.tuberlin.pserver.math.matrix.MatrixBuilder;
import de.tuberlin.pserver.math.tuples.Tuple2;
import de.tuberlin.pserver.runtime.MLProgram;
import org.apache.commons.lang3.mutable.MutableDouble;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.List;
import java.util.Random;

public class TSNEJob extends MLProgram {

    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final int ROWS = 10;
    private static final int INPUT_COLS = 28 * 28;
    private static final int EMBEDDING_DIMENSION = 2;

    private static final int NUM_EPOCHS = 200;
    private static final double PERPLEXITY = 2.0;
    private static final double LEARNING_RATE = 350.0;
    private static final double EARLY_EXAGGERATION = 1.0;

    private static final double INITIAL_MOMENTUM = 0.5;
    private static final double FINAL_MOMENTUM = 0.8;
    private static final double MIN_GAIN = 0.01;
    private static final double TOL = 1e-5;

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    @State(globalScope = GlobalScope.REPLICATED, rows = ROWS, cols = INPUT_COLS,
            path = "datasets/mnist_10_X.csv", format = Format.SPARSE_FORMAT)
    public Matrix X;

    @State(globalScope = GlobalScope.LOGICALLY_PARTITIONED, rows = ROWS, cols = ROWS,
            remoteUpdate = RemoteUpdate.COLLECT_PARTITIONS_UPDATE)
    public Matrix P;

    @State(globalScope = GlobalScope.REPLICATED, rows = ROWS, cols = EMBEDDING_DIMENSION)
    public Matrix Y;

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Unit
    public void main(final Program program) {

        program.initialize(() -> {

            final Random rand = new Random();
            Y.applyOnElements(e -> e = rand.nextDouble());

        }).process(() -> {

            P.assign(binarySearch(X, TOL, PERPLEXITY));
            P.assign(P.add(P.transpose()));

           // SINGLETON OPERATION !!!
            double sumP = P.sum();

            P.assign(P.scale(1 / sumP));

            // early exaggeration
            P.assign(P.scale(EARLY_EXAGGERATION));

            final long n = Y.rows();
            final long d = Y.cols();

            final Matrix Q            = new MatrixBuilder().dimension(n, n).build();
            final Matrix Y_squared    = new MatrixBuilder().dimension(n, d).build();
            final Matrix gains        = new MatrixBuilder().dimension(n, d).build();
            final Matrix iY           = new MatrixBuilder().dimension(n, d).build();
            final Matrix dY           = new MatrixBuilder().dimension(n, d).build();
            final Matrix mean         = new MatrixBuilder().dimension(1, d).build();

            gains.assign(1.0);

            final MutableDouble momentum = new MutableDouble(0.0);

            CF.loop()
                    .exe(NUM_EPOCHS, (epoch) -> {

                        Y.applyOnElements(e -> Math.pow(e, 2), Y_squared);
                        final Matrix sum_Y = Y_squared.aggregateRows(Matrix::sum);
                        final Matrix num = Y.mul(Y.transpose())
                                .scale(-2)
                                .addVectorToRows(sum_Y)
                                .transpose()
                                .addVectorToRows(sum_Y)
                                .applyOnElements(e -> 1.0 / (e + 1.0));

                        num.setDiagonalsToZero(num);

                        // ---------------------------------------------------
                        // (2) SINGLETON OPERATION!!!
                        // ---------------------------------------------------
                        final double sumNum = num.sum();

                        num.scale(1.0 / sumNum, Q);
                        Q.applyOnElements(e -> Math.max(e, 1e-12), Q);
                        final Matrix PQ = P.sub(Q);

                        CF.loop()
                                .exe(P.rows(), (i) -> {
                                    final Matrix sumVec = new MatrixBuilder().dimension(1, d).build();
                                    CF.loop()
                                            .exe(P.cols(), (j) -> {
                                                final Double pq = PQ.get(i, j);
                                                final Double num_j = num.get(i, j);
                                                sumVec.assign(
                                                        sumVec.add(
                                                                Y.getRow(i)
                                                                        .sub(Y.getRow(j))
                                                                        .applyOnElements(e -> e * pq * num_j)
                                                        )
                                                );
                                            });
                                    dY.assignRow(i, sumVec);
                                });

                        momentum.setValue((epoch < 20) ? INITIAL_MOMENTUM : FINAL_MOMENTUM);

                        CF.loop()
                                .exe(gains, (e, i, j, v) -> {
                                    final Double dY_j = dY.get(i, j);
                                    final Double iY_j = iY.get(i, j);
                                    final Double gain_j = gains.get(i, j);
                                    gains.set(i, j, (dY_j > 0) == (iY_j > 0) ? gain_j * 0.8 : gain_j + 0.2);
                                });

                        gains.assign(gains.applyOnElements(e -> Math.max(e, MIN_GAIN)));

                        iY.assign(
                                iY.scale(momentum.getValue())
                                        .sub(dY.applyOnElements(gains, (e1, e2) -> e1 * e2).scale(LEARNING_RATE))
                        );

                        Y.add(iY, Y);
                        mean.assign(0.0);
                        for (int i = 0; i < Y.rows(); ++i)
                            mean.add(Y.getRow(i));
                        mean.scale(-1./Y.rows(), mean);
                        Y.addVectorToRows(mean, Y);

                        if ((epoch + 1) % 10 == 0) {
                            double C = 0.0;
                            for (int i = 0; i < P.rows(); ++i) {
                                for (int j = 0; j < P.cols(); ++j) {
                                    if (i != j) {
                                        C += P.get(i, j) * Math.log(P.get(i, j) / Q.get(i, j));
                                    }
                                }
                            }
                            LOG.info("Iteration " + (epoch + 1) + ", Error: " + C);
                        }

                        if (epoch == 100)
                            P.assign(P.scale(1.0 / EARLY_EXAGGERATION));

                        LOG.debug("Y: " + Y.getRow(0).toString());
                    });

        }).postProcess(() -> result(Y));
    }

    // ---------------------------------------------------
    // Helper Methods.
    // ---------------------------------------------------

    private Matrix binarySearch(Matrix X, Double tol, Double perplexity) {

        long n = X.rows();
        long d = X.cols();

        final Matrix X_squared  = new MatrixBuilder().dimension(n, d).build();
        final Matrix P_tmp      = new MatrixBuilder().dimension(n, n).build();
        final Matrix beta       = new MatrixBuilder().dimension(1, n).build();

        beta.assign(1.0);

        // compute the distances between all x_i
        X_squared.assign(X_squared.applyOnElements(e -> Math.pow(e, 2), X));

        final Matrix sumX = X_squared.aggregateRows(Matrix::sum);
        final Matrix D = X.mul(X.transpose()).scale(-2).addVectorToRows(sumX).transpose().addVectorToRows(sumX);

        final double logU = Math.log(perplexity);

        for (long i=0; i < n; ++i) {

            double betaMin = Double.NEGATIVE_INFINITY;
            double betaMax = Double.POSITIVE_INFINITY;

            final Matrix Di = D.getRow(i);

            Tuple2<Double, Matrix> hBeta = computeHBeta(Di, beta.get(i), i);

            double H = hBeta._1;
            Matrix Pi = hBeta._2;
            // Evaluate whether the perplexity is within tolerance

            double HDiff = H - logU;
            int tries = 0;

            while(Math.abs(HDiff) > tol && tries++ < 50){

                if (HDiff > 0){
                    betaMin = beta.get(0, i);
                    if (Double.isInfinite(betaMax))
                        beta.set(0, i, beta.get(i) * 2);
                    else
                        beta.set(0, i, (beta.get(i) + betaMax) / 2);
                } else {
                    betaMax = beta.get(0, i);
                    if (Double.isInfinite(betaMin))
                        beta.set(0, i, beta.get(i) / 2);
                    else
                        beta.set(0, i, (beta.get(i) + betaMin) / 2);
                }

                hBeta = computeHBeta(Di, beta.get(i), i);
                H = hBeta._1;
                Pi = hBeta._2;
                HDiff = H - logU;
            }

            P_tmp.assignRow(i, Pi);
        }

        return P_tmp;
    }

    private Tuple2<Double, Matrix> computeHBeta(Matrix d, Double beta, Long index){
        final Matrix P = new MatrixBuilder().dimension(1, d.cols()).build();
        P.set(0, index, 0.0);

        // parExe over all elements i != j
        for (long i=0; i < P.cols(); ++i)
            if (i != index)
                P.set(0, i, Math.exp(-1 * d.get(i) * beta));

        final Matrix PD = P.copy().applyOnElements(d, (e1, e2) -> e1 * e2);
        final double sumP = P.sum();
        final double H = Math.log(sumP) + beta * PD.sum() / sumP;

        P.assign(P.applyOnElements(e -> e / sumP));

        return new Tuple2<>(H, P);
    }

    // ---------------------------------------------------
    // Entry Point.
    // ---------------------------------------------------

    public static void main(final String[] args) {

        final List<List<Serializable>> res = Lists.newArrayList();

        PServerExecutor.LOCAL
                .run(TSNEJob.class)
                .results(res)
                .done();

        try (PrintWriter writer = new PrintWriter("datasets/pserver_mnist.csv", "UTF-8")) {

            Matrix R = (Matrix) res.get(0).get(0);
            for (int i = 0; i < R.rows(); ++i) {
                for (int j = 0; j < R.cols(); ++j) {
                    writer.println(i + "," + j + "," + R.get(i, j));
                }
            }

        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }
}