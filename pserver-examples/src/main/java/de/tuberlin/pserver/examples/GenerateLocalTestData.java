package de.tuberlin.pserver.examples;

import de.tuberlin.pserver.ml.generators.DataGenerator;

public class GenerateLocalTestData {

    public static void main(final String[] args) {

        DataGenerator.generateDatasetAndWriteToFile2(1000, 15, 42, "datasets/demo_dataset.csv");
    }
}
