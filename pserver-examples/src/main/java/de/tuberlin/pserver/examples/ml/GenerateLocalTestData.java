package de.tuberlin.pserver.examples.ml;

import de.tuberlin.pserver.ml.generators.DataGenerator;

public class GenerateLocalTestData {

    public static void main(final String[] args) {

        //DataGenerator.generateDatasetAndWriteToFile(1000, 15, 42, "datasets/demo_dataset.csv");

        DataGenerator.generateDatasetAndWriteToFile(5000, 1000, 42, "datasets/sparse_dataset.csv");
    }
}
