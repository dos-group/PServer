package de.tuberlin.pserver.ml.generators;

import de.tuberlin.pserver.math.vector.Vector;
import de.tuberlin.pserver.math.vector.dense.DVector;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public final class DataGenerator {

    // ---------------------------------------------------

    private DataGenerator() {}

    // ---------------------------------------------------

    public static double[][] generateDataset(final int numExamples,
                                             final int numFeatures,
                                             final long seed) {

        final Random rand = new Random();
        rand.setSeed(seed);
        final double[][] data = new double[numExamples][numFeatures + 1];
        for (int i = 0; i < numExamples; ++i) {
            for (int j = 0; j < numFeatures; ++j) {
                data[i][j] = rand.nextGaussian();

            }
            data[i][numFeatures] = rand.nextGaussian();
        }
        return data;
    }

    // ---------------------------------------------------

    public static Pair<double[][], double[]> generateDataset2(final int numExamples,
                                                              final int numFeatures,
                                                              final long seed) {
        final Random rand = new Random();
        rand.setSeed(seed);
        final double[] params = new double[numFeatures];
        //params[0] = 1;
        for(int l = 0; l < params.length; l++)
            params[l] = rand.nextDouble();

        final double[][] data = new double[numExamples][numFeatures + 1];
        for (int i = 0; i < numExamples; ++i) {
            double label = 0;
            for (int j = 0; j < numFeatures; ++j) {
                final double value = Math.abs(rand.nextGaussian());
                data[i][j] = value;
                label += value * params[j];
            }
            data[i][numFeatures] = label + Math.abs(rand.nextGaussian());
        }
        return Pair.of(data, params);
    }

    // ---------------------------------------------------

    /*public static double[] generateDatasetAndWriteToFile(final int numExamples,
                                                         final int numFeatures,
                                                         final long seed,
                                                         final String fileName) {
        final Random rand = new Random();
        rand.setSeed(seed);

        FileWriter parameterFW              = null;
        CSVPrinter parameterCSVPrinter      = null;
        FileWriter trainingDataFW           = null;
        CSVPrinter trainingDataCSVPrinter   = null;
        final CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator("\n");

        try {
            parameterFW = new FileWriter(fileName + ".param");
            parameterCSVPrinter = new CSVPrinter(parameterFW, csvFileFormat);
            List<Double> params = new ArrayList<>();
            for(int l = 0; l < numFeatures; l++) {
                params.add(rand.nextDouble());
            }

            parameterCSVPrinter.printRecord(params);
            parameterFW.flush();
            parameterFW.close();
            parameterCSVPrinter.close();

            trainingDataFW = new FileWriter(fileName);
            trainingDataCSVPrinter = new CSVPrinter(trainingDataFW, csvFileFormat);
            final List<Double> data = new ArrayList<>(numFeatures + 1);

            for (int i = 0; i < numExamples; ++i) {

                double label = 0;

                data.add(1.0);
                for (int j = 1; j < numFeatures; ++j) {
                    //final double value = rand.nextGaussian();
                    final double value = rand.nextGaussian() * params.get(j);
                    data.add(j, value);
                }

                for (int k = 0; k < numFeatures; ++k) {
                    //label += data.get(k) * params.get(k);
                    label += data.get(k);
                }

                data.add(numFeatures, label); //+ Math.abs(rand.nextGaussian()));
                trainingDataCSVPrinter.printRecord(data);
                data.clear();
            }

            return ArrayUtils.toPrimitive(params.toArray(new Double[numFeatures]));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            try {
                if (trainingDataFW != null) {
                    trainingDataFW.flush();
                    trainingDataFW.close();
                }
                if (trainingDataCSVPrinter != null)
                    trainingDataCSVPrinter.close();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }*/

    public static double[] generateDatasetAndWriteToFile(final int numExamples,
                                                         final int numFeatures,
                                                         final long seed,
                                                         final String fileName) {
        final Random rand = new Random();
        rand.setSeed(seed);

        FileWriter parameterFW              = null;
        CSVPrinter parameterCSVPrinter      = null;
        FileWriter trainingDataFW           = null;
        CSVPrinter trainingDataCSVPrinter   = null;
        final CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator("\n");

        try {
            parameterFW = new FileWriter(fileName + ".param");
            parameterCSVPrinter = new CSVPrinter(parameterFW, csvFileFormat);
            List<Double> params = new ArrayList<>();
            params.add((double)0);
            for(int l = 1; l < numFeatures; l++) {
                params.add(rand.nextDouble());
            }

            parameterCSVPrinter.printRecord(params);
            parameterFW.flush();
            parameterFW.close();
            parameterCSVPrinter.close();

            trainingDataFW = new FileWriter(fileName);
            trainingDataCSVPrinter = new CSVPrinter(trainingDataFW, csvFileFormat);
            final List<Double> data = new ArrayList<>(numFeatures + 1);

            for (int i = 0; i < numExamples; ++i) {
                double label = 0;
                for (int j = 0; j < numFeatures; ++j) {
                    final double value = rand.nextGaussian();
                    data.add(j, value);
                    label += value * params.get(j);
                }
                data.add(numFeatures, label + Math.abs(rand.nextGaussian()));
                trainingDataCSVPrinter.printRecord(data);
                data.clear();
            }

            return ArrayUtils.toPrimitive(params.toArray(new Double[numFeatures]));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            try {
                if (trainingDataFW != null) {
                    trainingDataFW.flush();
                    trainingDataFW.close();
                }
                if (trainingDataCSVPrinter != null)
                    trainingDataCSVPrinter.close();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    // ---------------------------------------------------

    public static double[] generateSparseDatasetAndWriteToFile(final int numExamples,
                                                               final int numFeatures,
                                                               final long seed,
                                                               final String fileName) {
        final Random rand = new Random();
        rand.setSeed(seed);

        FileWriter parameterFW              = null;
        CSVPrinter parameterCSVPrinter      = null;
        FileWriter trainingDataFW           = null;
        CSVPrinter trainingDataCSVPrinter   = null;
        final CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator("\n");

        try {
            parameterFW = new FileWriter(fileName + ".param");
            parameterCSVPrinter = new CSVPrinter(parameterFW, csvFileFormat);
            List<Double> params = new ArrayList<>();
            params.add((double)0);
            for(int l = 1; l < numFeatures; l++) {
                if (rand.nextDouble() <= 0.5)
                    params.add(rand.nextDouble());
                else
                    params.add(0.0);
            }

            parameterCSVPrinter.printRecord(params);
            parameterFW.flush();
            parameterFW.close();
            parameterCSVPrinter.close();

            trainingDataFW = new FileWriter(fileName);
            trainingDataCSVPrinter = new CSVPrinter(trainingDataFW, csvFileFormat);
            final List<Double> data = new ArrayList<>(numFeatures + 1);

            for (int i = 0; i < numExamples; ++i) {
                double label = 0;
                for (int j = 0; j < numFeatures; ++j) {
                    final double value = rand.nextGaussian();
                    data.add(j, value);
                    label += value * params.get(j);
                }
                data.add(numFeatures, label + Math.abs(rand.nextGaussian()));
                trainingDataCSVPrinter.printRecord(data);
                data.clear();
            }

            return ArrayUtils.toPrimitive(params.toArray(new Double[numFeatures]));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            try {
                if (trainingDataFW != null) {
                    trainingDataFW.flush();
                    trainingDataFW.close();
                }
                if (trainingDataCSVPrinter != null)
                    trainingDataCSVPrinter.close();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    // ---------------------------------------------------

    public static double[] generateLinearData(final String fileName,
                                              final long nFeatures,
                                              final int nPoints,
                                              final double eps) {

        final Random rand = new Random();
        rand.setSeed(42);
        final Vector weights = new DVector(nFeatures);
        for (long i = 0; i < weights.length(); ++i)
            weights.set(i, rand.nextDouble() - 0.5);
        final Vector xMean = new DVector(nFeatures);
        xMean.assign(0.0);
        final Vector xVariance = new DVector(nFeatures);
        xVariance.assign(1.0 / 3.0);
        return generateLinearData(fileName, 42, 0.0, weights, xMean, xVariance, nPoints, eps);
    }

    public static double[] generateLinearData(final String fileName,
                                              final long seed,
                                              final double intercept,
                                              final Vector weights,
                                              final Vector xMean,
                                              final Vector xVariance,
                                              final int nPoints,
                                              double eps) {

        final Random rand = new Random();
        rand.setSeed(seed);

        final Vector[] x = new Vector[nPoints];
        for (int i = 0; i < nPoints; ++i) {
            x[i] = new DVector(weights.length());
            for (int j = 0; j < (int)weights.length(); ++j) {
                x[i].set(j, (rand.nextDouble() - 0.5) * Math.sqrt(12.0 * xVariance.get(j)) + xMean.get(j));
            }
        }

        final double[] y = new double[nPoints];
        for (int i = 0; i < nPoints; ++i) {
            y[i] = x[i].dot(weights) + intercept + eps * rand.nextGaussian();
        }

        FileWriter trainingDataFW           = null;
        CSVPrinter trainingDataCSVPrinter   = null;
        final CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator("\n");

        try {
            trainingDataFW = new FileWriter(fileName);
            trainingDataCSVPrinter = new CSVPrinter(trainingDataFW, csvFileFormat);
            final List<Double> data = new ArrayList<>((int)weights.length() + 1);
            for (int i = 0; i < nPoints; ++i) {
                for (int j = 0; j < (int)weights.length(); ++j) {
                    data.add(x[i].get(j));
                }
                data.add(y[i]);
                trainingDataCSVPrinter.printRecord(data);
                data.clear();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (trainingDataFW != null) {
                    trainingDataFW.flush();
                    trainingDataFW.close();
                }
                if (trainingDataCSVPrinter != null)
                    trainingDataCSVPrinter.close();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        return weights.toArray();
    }

    public static void generateRowColValuePerLineDatasetAndWriteToFile(final int numExamples,
                                                                       final int numFeatures,
                                                                       final long seed,
                                                                       final String fileName) {

        final Random rand = new Random();
        rand.setSeed(seed);

        FileWriter parameterFW              = null;
        CSVPrinter parameterCSVPrinter      = null;
        FileWriter trainingDataFW           = null;
        CSVPrinter trainingDataCSVPrinter   = null;
        final CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator("\n");

        try {
            trainingDataFW = new FileWriter(fileName);
            trainingDataCSVPrinter = new CSVPrinter(trainingDataFW, csvFileFormat);

            //final List<Object[]> data = new ArrayList<Object[]>();
            Object[] reusable = new Object[3];
            for (int i = 0; i < numExamples; ++i) {
                for (int j = 0; j < numFeatures; ++j) {
                    final double value = rand.nextGaussian();
                    //data.add(new Object[] {i, j, value});
                    reusable[0] = i;
                    reusable[1] = j;
                    reusable[2] = value;
                    trainingDataCSVPrinter.printRecord(reusable);
                }
            }
            //Collections.shuffle(data);
            //for(Object[] point : data) {
                //trainingDataCSVPrinter.printRecord(point);
            //}

        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            try {
                if (trainingDataFW != null) {
                    trainingDataFW.flush();
                    trainingDataFW.close();
                }
                if (trainingDataCSVPrinter != null)
                    trainingDataCSVPrinter.close();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

    }
}
