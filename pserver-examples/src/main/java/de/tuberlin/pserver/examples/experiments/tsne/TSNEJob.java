package de.tuberlin.pserver.examples.experiments.tsne;

import de.tuberlin.pserver.runtime.MLProgram;

public class TSNEJob extends MLProgram {
/*
    // ---------------------------------------------------
    // Constants.
    // ---------------------------------------------------

    private static final int INPUT_ROWS = 10;
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

    @State(globalScope = State.PARTITIONED_INPUT, rows = INPUT_ROWS, cols = INPUT_COLS, path = "datasets/mnist_10_X.csv", format = Format.SPARSE_FORMAT, layout = Layout.ROW_LAYOUT)
    public Matrix X;

    @State(globalScope = State.REPLICATED, rows = INPUT_ROWS, cols = EMBEDDING_DIMENSION, layout = Layout.ROW_LAYOUT, format = Format.DENSE_FORMAT)
    public Matrix Y;

    private Matrix P;

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public void define(final Program program) {

        program.initialize(() -> {

            final Random rand = new Random();
            Y.applyOnElements(e -> e = rand.nextDouble());

        }).process(() -> {

            Matrix Xsquared = new MatrixBuilder().dimension(X.numRows(), Y.numCols()).build();
            Vector sumX = Xsquared.aggregateRows(Vector::sum);
            Matrix D = X.mul(X.transpose()).scale(-2).addVectorToRows(sumX).transpose().addVectorToRows(sumX);

            P = binarySearch(X, TOL, PERPLEXITY);
            P = P.add(P.transpose());

            // ---------------------------------------------------
            // (1) GLOBAL OPERATION!!!
            // ---------------------------------------------------
            double sumP = P.aggregateRows(Vector::sum).sum();

            P = P.scale(1 / sumP);

            // early exaggeration
            P = P.scale(EARLY_EXAGGERATION);

            final long n = Y.numRows();
            final long d = Y.numCols();

            Matrix Q = new MatrixBuilder().dimension(n, n).build();
            Matrix Y_squared = new MatrixBuilder().dimension(n, d).build();
            Matrix gains = new MatrixBuilder().dimension(n, d).build();
            Matrix iY = new MatrixBuilder().dimension(n, d).build();
            Matrix dY = new MatrixBuilder().dimension(n, d).build();
            Vector mean = new VectorBuilder().dimension(d).build();

            gains.assign(1.0);
            iY.assign(0.0);

            final MutableDouble momentum = new MutableDouble(0.0);

            CF.iterate()
                    .sync(Iteration.GLOBAL | Iteration.LOCAL)
                    .exe(NUM_EPOCHS, (epoch) -> {
                        // ! compare git diff
                        Y.applyOnElements(e -> Math.pow(e, 2), Y_squared);
                        final Vector sum_Y = Y_squared.aggregateRows(Vector::sum);
                        final Matrix num = Y.mul(Y.transpose())
                                .scale(-2)
                                .addVectorToRows(sum_Y)
                                .transpose()
                                .addVectorToRows(sum_Y)
                                .applyOnElements(e -> 1.0 / (e + 1.0));

                        num.setDiagonalsToZero(num);

                        // ---------------------------------------------------
                        // (2) GLOBAL OPERATION!!!
                        // ---------------------------------------------------
                        final double sumNum = num.aggregateRows(Vector::sum).sum();

                        Q.assign(num.copy().scale(1.0 / sumNum));
                        Q.assign(Q.applyOnElements(e -> Math.max(e, 1e-12)));
                        final Matrix PQ = P.copy().sub(Q);

                        CF.iterate()
                                .exe(P.numRows(), (i) -> {
                                    final Vector sumVec = new VectorBuilder().dimension(d).build();
                                    CF.iterate()
                                            .exe(P.numCols(), (j) -> {
                                                final Double pq = PQ.get(i, j);
                                                final Double num_j = num.get(i, j);
                                                sumVec.assign(
                                                        sumVec.add(
                                                                Y.rowAsVector(i)
                                                                        .sub(Y.rowAsVector(j))
                                                                        .applyOnElements(e -> e * pq * num_j)
                                                        )
                                                );
                                            });
                                    dY.assignRow(i, sumVec);
                                });

                        momentum.setValue((epoch < 20) ? INITIAL_MOMENTUM : FINAL_MOMENTUM);

                        CF.iterate()
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

                        Y.assign(Y.add(iY));
                        mean.assign(0.0);
                        for (int i = 0; i < Y.numRows(); ++i)
                            mean.add(Y.rowAsVector(i));
                        mean.assign(mean.div(Y.numRows()));
                        Y.assign(Y.addVectorToRows(mean.mul(-1.0)));

                        if ((epoch + 1) % 10 == 0) {
                            double C = 0.0;
                            for (int i = 0; i < P.numRows(); ++i) {
                                for (int j = 0; j < P.numCols(); ++j) {
                                    if (i != j) {
                                        C += P.get(i, j) * Math.log(P.get(i, j) / Q.get(i, j));
                                    }
                                }
                            }
                            LOG.info("Iteration " + (epoch + 1) + ", Error: " + C);
                        }

                        if (epoch == 100)
                            P.assign(P.scale(1.0 / EARLY_EXAGGERATION));

                        LOG.debug("Y: " + Y.rowAsVector().toString());
                    });

        }).postProcess(() -> result(Y));
    }

    // ---------------------------------------------------
    // Helper Methods.
    // ---------------------------------------------------

    private Matrix binarySearch(Matrix X, Double tol, Double perplexity) {
        long n = X.numRows();
        long d = X.numCols();
        Matrix Xsquared = new MatrixBuilder().dimension(n, d).build();
        Matrix P = new MatrixBuilder().dimension(n, n).build();
        Vector beta = new VectorBuilder().dimension(n).build();
        P.assign(0.0);
        beta.assign(1.0);
        // compute the distances between all x_i
        Xsquared = Xsquared.applyOnElements(e -> Math.pow(e, 2), X);

        double logU = Math.log(perplexity);
        for (long i=0; i < n; ++i) {
            double betaMin = Double.NEGATIVE_INFINITY;
            double betaMax = Double.POSITIVE_INFINITY;
            Vector Di = D.rowAsVector(i);
            Tuple2<Double, Vector> hBeta = HBeta(Di, beta.get(i), i);
            double H = hBeta._1;
            Vector Pi = hBeta._2;
            // Evaluate whether the perplexity is within tolerance
            double HDiff = H - logU;
            int tries = 0;
            while(Math.abs(HDiff) > tol && tries < 50){
                if (HDiff > 0){
                    betaMin = beta.get(i);
                    if (Double.isInfinite(betaMax))
                        beta.set(i, beta.get(i) * 2);
                    else
                        beta.set(i, (beta.get(i) + betaMax) / 2);
                } else{
                    betaMax = beta.get(i);
                    if (Double.isInfinite(betaMin))
                        beta.set(i, beta.get(i) / 2);
                    else
                        beta.set(i, (beta.get(i) + betaMin) / 2);
                }
                hBeta = HBeta(Di, beta.get(i), i);
                H = hBeta._1;
                Pi = hBeta._2;
                HDiff = H - logU;
                tries = tries + 1;
            }
            P.assignRow(i, Pi);
        }
        return P;
    }

    private Tuple2<Double, Vector> HBeta (Vector d, Double beta, Long index){
        Vector P = new VectorBuilder().dimension(d.length()).build();
        P.set(index, 0.0);
        // parExe over all elements i != j
        for (long i=0; i < P.length(); ++i) {
            if (i != index) {
                P.set(i, Math.exp(-1 * d.get(i) * beta));
            }
        }
        Vector PD = P.copy().applyOnElements(d, (e1, e2) -> e1 * e2);
        double sumP = P.sum();
        double H = Math.log(sumP) + beta * PD.sum() / sumP;
        P = P.applyOnElements(e -> e / sumP);
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
            for (int i = 0; i < R.numRows(); ++i) {
                for (int j = 0; j < R.numCols(); ++j) {
                    writer.println(i + "," + j + "," + R.get(i, j));
                }
            }

        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }*/
}