package de.tuberlin.pserver.ml.optimization.naive;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
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

    public static void generateDatasetAndWriteToFile(final int numExamples,
                                                     final int numFeatures,
                                                     final long seed,
                                                     final String fileName) {

        FileWriter fileWriter = null;
        CSVPrinter csvFilePrinter = null;
        final CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator("\n");

        try {
            fileWriter = new FileWriter(fileName);
            csvFilePrinter = new CSVPrinter(fileWriter, csvFileFormat);
            final Random gaussianRand = new Random();
            gaussianRand.setSeed(seed);
            final List<Double> data = new ArrayList<>(numFeatures + 1);
            //final double[] labels = new double[numExamples];
            for (int i = 0; i < numExamples; ++i) {
                for (int j = 0; j < numFeatures; ++j)
                    data.add(j, gaussianRand.nextGaussian());
                data.add(numFeatures, gaussianRand.nextGaussian());
                csvFilePrinter.printRecord(data);
                data.clear();
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            try {
                if (fileWriter != null) {
                    fileWriter.flush();
                    fileWriter.close();
                }
                if (csvFilePrinter != null) csvFilePrinter.close();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    // ---------------------------------------------------

    public static double[] generateDatasetAndWriteToFile2(final int numExamples,
                                                          final int numFeatures,
                                                          final long seed,
                                                          final String fileName) {
        final Random rand = new Random();
        rand.setSeed(seed);

        FileWriter fileWriter = null;
        CSVPrinter csvFilePrinter = null;
        final CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator("\n");

        try {
            final double[] params = new double[numFeatures];
            for(int l = 1; l < params.length; l++)
                params[l] = rand.nextDouble();

            fileWriter = new FileWriter(fileName);
            csvFilePrinter = new CSVPrinter(fileWriter, csvFileFormat);
            final List<Double> data = new ArrayList<>(numFeatures + 1);

            for (int i = 0; i < numExamples; ++i) {
                double label = 0;
                for (int j = 0; j < numFeatures; ++j) {
                    final double value = rand.nextGaussian();
                    data.add(j, value);
                    label += value * params[j];
                }
                data.add(numFeatures, label + Math.abs(rand.nextGaussian()));
                csvFilePrinter.printRecord(data);
                data.clear();
            }

            return params;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            try {
                if (fileWriter != null) {
                    fileWriter.flush();
                    fileWriter.close();
                }
                if (csvFilePrinter != null) csvFilePrinter.close();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
