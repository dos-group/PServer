package de.tuberlin.pserver.regression;


import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;
import de.tuberlin.pserver.math.matrix.Matrix32F;
import de.tuberlin.pserver.math.matrix.dense.DenseMatrix32F;

public class ExactAvgRegressionDenseMatrix32FTestJob extends Program {
    private static final int ROWS = 10;
    private static final int COLS = 10;
    private static final String NUM_NODES = "2";


    private static void fillMatrix(Matrix32F matrix) {
        for(int i = 0; i < ROWS; i++) {
            for(int k = 0; k < COLS; k++) {
                matrix.set(i, k, (float)(i + k));
            }
        }
    }


    @Unit(at = "0")
    public void test1(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            DenseMatrix32F regMatrix = new DenseMatrix32F(ROWS, COLS);
            ExactAvgRegressionDenseMatrix32F crdtMatrix = new ExactAvgRegressionDenseMatrix32F(ROWS, COLS, "blub", Integer.valueOf(NUM_NODES), programContext);

            fillMatrix(regMatrix);
            fillMatrix(crdtMatrix);

            DenseMatrix32F b = new DenseMatrix32F(ROWS, COLS);
            fillMatrix(b);

            /*System.out.println("B: " + b);
            System.out.println("R: " + regMatrix);
            System.out.println("C: " + crdtMatrix);
            System.out.println("R - Dot: " + regMatrix.dot(b));
            System.out.println("C - Dot: " + crdtMatrix.dot(b));
            System.out.println("B: " + b);
            System.out.println("R: " + regMatrix);
            System.out.println("C: " + crdtMatrix);*/


            System.out.println("R: " + regMatrix);
            regMatrix.set(0, 1, 99.0f);
            System.out.println("1 - get(11): "+regMatrix.get(11));
            System.out.println("1 - get(2,4): "+regMatrix.get(2,4));
            // apply on elements
            System.out.println("R: " + regMatrix);
            regMatrix.sub(b, regMatrix);
            System.out.println("R: " + regMatrix);
            /*regMatrix.sub(b);
            System.out.println("R: " + regMatrix);*/
            regMatrix.scale(1.5f);
            System.out.println("R Scale: " + regMatrix);
            System.out.println("1 - Dot: " + regMatrix.dot(b));


            System.out.println("C: " + crdtMatrix);
            crdtMatrix.set(0, 1, 99.0f);
            System.out.println("2 - get(11): "+crdtMatrix.get(11));
            System.out.println("2 - get(2,4): "+crdtMatrix.get(2,4));
            // apply on elements
            System.out.println("C: " + crdtMatrix);
            crdtMatrix.sub(b, crdtMatrix);
            System.out.println("C: " + crdtMatrix);
            /*crdtMatrix.sub(b);
            System.out.println("C: " + crdtMatrix);*/
            crdtMatrix.scale(1.5f);
            System.out.println("C Scale: " + crdtMatrix);
            System.out.println("2 - Dot: " + crdtMatrix.dot(b));
            //crdtMatrix.subAndBroadcast(b, crdtMatrix);

            crdtMatrix.finish();

            System.out.println("1: " + regMatrix);
            System.out.println("1: " + crdtMatrix);

        });
    }

    @Unit(at = "1")
    public void test2(Lifecycle lifecycle) {
        lifecycle.process(() -> {
            DenseMatrix32F regMatrix = new DenseMatrix32F(ROWS, COLS);
            ExactAvgRegressionDenseMatrix32F crdtMatrix = new ExactAvgRegressionDenseMatrix32F(ROWS, COLS, "blub", Integer.valueOf(NUM_NODES), programContext);

           /* fillMatrix(regMatrix);
            fillMatrix(crdtMatrix);

            DenseMatrix32F b = new DenseMatrix32F(ROWS, COLS);
            fillMatrix(b);

            regMatrix.set(0, 1, 99.0f);
            crdtMatrix.set(0, 1, 99.0f);

            regMatrix.sub(b, regMatrix);
            //crdtMatrix.subAndBroadcast(b, crdtMatrix);*/

            crdtMatrix.finish();

            System.out.println("2: " + regMatrix);
            System.out.println("2: " + crdtMatrix);

        });
    }

    public static void main(String[] args) {

        // Set the number of simulated nodes, can also be
        // configured via 'pserver/pserver-core/src/main/resources/reference.simulation.conf'
        System.setProperty("simulation.numNodes", NUM_NODES);
        // Set the memory each simulated node gets.
        System.setProperty("jvmOptions", "[\"-Xmx256m\"]");

        PServerExecutor.LOCAL
                // Second param is number of slots (threads executing the job) per node,
                // should be 1 at the beginning.
                .run(ExactAvgRegressionDenseMatrix32FTestJob.class)
                .done();
    }
}
