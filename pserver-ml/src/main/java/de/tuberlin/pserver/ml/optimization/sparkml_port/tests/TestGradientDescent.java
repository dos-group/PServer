package de.tuberlin.pserver.ml.optimization.sparkml_port.tests;


import de.tuberlin.pserver.core.filesystem.FileDataIterator;
import de.tuberlin.pserver.core.filesystem.local.LocalCSVInputFile;
import de.tuberlin.pserver.math.experimental.tuples.Tuple2;
import de.tuberlin.pserver.ml.optimization.sparkml_port.Gradient;
import de.tuberlin.pserver.ml.optimization.sparkml_port.GradientDescent;
import de.tuberlin.pserver.ml.optimization.sparkml_port.Updater;
import org.apache.commons.csv.CSVRecord;
import org.jblas.DoubleMatrix;

import java.util.ArrayList;
import java.util.List;

public class TestGradientDescent {

    // Disallow instantiation.
    private TestGradientDescent() {}

    public static void main(final String[] args) {

        final LocalCSVInputFile fileSection = new LocalCSVInputFile("datasets/parkinsons_updrs.data", "\n", ',');
        final FileDataIterator<CSVRecord> fileIterator = fileSection.iterator();
        fileSection.computeLocalFileSection(3, 0);


        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        while (fileIterator.hasNext()) {
            fileIterator.next();
        }


        /*int numFeatures = -1;
        boolean isLabelFirstOrLastElement = true;

        final List<Tuple2<Double,DoubleMatrix>> trainingSet = new ArrayList<>();
        while (fileIterator.hasNext()) {
            final CSVRecord record = fileIterator.next();

            numFeatures = record.size() - 1;
            final double[] featuresArr = new double[numFeatures];
            for (int i = 0; i < numFeatures; ++i) {
                featuresArr[i] = Double.parseDouble(record.get(i + (isLabelFirstOrLastElement ? 1 : 0)));
            }

            final DoubleMatrix features = new DoubleMatrix(1, numFeatures, featuresArr);
            final double label = Double.parseDouble(record.get(isLabelFirstOrLastElement ? 0 : numFeatures));
            final Tuple2<Double,DoubleMatrix> example = new Tuple2<>(label, features);
            trainingSet.add(example);
        }

        final Updater updater   = new Updater.SimpleUpdater();
        final Gradient gradient = new Gradient.LeastSquaresGradient();
        final DoubleMatrix initialWeights = new DoubleMatrix(numFeatures);
        final GradientDescent sgd = new GradientDescent(gradient, updater, initialWeights);
        final Tuple2<DoubleMatrix, double[]> result = sgd.runMiniBatchSGD(trainingSet);

        final DoubleMatrix model = result._1;
        for (int i = 0; i < model.length; ++i) {
            System.out.print(model.get(i));
            System.out.print(" ");
        }*/
    }
}
