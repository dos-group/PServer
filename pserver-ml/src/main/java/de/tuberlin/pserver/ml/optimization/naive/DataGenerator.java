package de.tuberlin.pserver.ml.optimization.naive;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

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

        final Random gaussianRand = new Random();
        gaussianRand.setSeed(seed);
        final double[][] data = new double[numExamples][numFeatures + 1];
        //final double[] labels = new double[numExamples];
        for (int i = 0; i < numExamples; ++i) {
            for (int j = 0; j < numFeatures; ++j) {
                data[i][j] = gaussianRand.nextGaussian();

            }
            data[i][numFeatures] = gaussianRand.nextGaussian();
        }
        return data;
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
}
