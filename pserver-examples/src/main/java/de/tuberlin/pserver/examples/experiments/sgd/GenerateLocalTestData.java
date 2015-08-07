package de.tuberlin.pserver.examples.experiments.sgd;

import de.tuberlin.pserver.ml.generators.DataGenerator;

public class GenerateLocalTestData {

    public static final int ROWS_SPARSE_DATASET = 5;
    public static final int COLS_SPARSE_DATASET = 1000000;

    public static final int ROWS_ROWCOLVAL_DATASET = 10000;
    public static final int COLS_ROWCOLVAL_DATASET = 2500;

    public static final int ROWS_DEMO_DATASET = 100;
    public static final int COLS_DEMO_DATASET = 3;

    public static void main(final String[] args) {

        //DataGenerator.generateDatasetAndWriteToFile(ROWS_DEMO_DATASET, COLS_DEMO_DATASET, 42, "datasets/demo_dataset.csv");

        //DataGenerator.generateDatasetAndWriteToFile(ROWS_SPARSE_DATASET, COLS_SPARSE_DATASET-1, 42, "datasets/sparse_dataset.csv");

        DataGenerator.generateRowColValuePerLineDatasetAndWriteToFile(ROWS_ROWCOLVAL_DATASET, COLS_ROWCOLVAL_DATASET, 42, "datasets/rowcolval_dataset_10000_2500.csv");
    }
}
