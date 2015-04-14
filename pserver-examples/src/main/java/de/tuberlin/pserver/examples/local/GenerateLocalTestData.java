package de.tuberlin.pserver.examples.local;

import de.tuberlin.pserver.ml.optimization.naive.DataGenerator;

public class GenerateLocalTestData {

    public static void main(final String[] args) {

        DataGenerator.generateDatasetAndWriteToFile2(300000, 15, 42, "datasets/data.csv");
    }
}
