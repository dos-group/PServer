package de.tuberlin.pserver.examples.ml;

import de.tuberlin.pserver.ml.generators.DataGenerator;

public class GenerateLocalTestData {

    public static final int ROWS_SPARSE_DATASET = 5;
    public static final int COLS_SPARSE_DATASET = 1000000;

    public static final int ROWS_ROWCOLVAL_DATASET = 10000;
    public static final int COLS_ROWCOLVAL_DATASET = 400;

    public static final int ROWS_DEMO_DATASET = 50;
    public static final int COLS_DEMO_DATASET = 3;

    public static void main(final String[] args) {

        //DataGenerator.generateDatasetAndWriteToFile(ROWS_DEMO_DATASET, COLS_DEMO_DATASET, 42, "datasets/demo_dataset.csv");

        //DataGenerator.generateDatasetAndWriteToFile(ROWS_SPARSE_DATASET, COLS_SPARSE_DATASET-1, 42, "datasets/sparse_dataset.csv");

        DataGenerator.generateRowColValuePerLineDatasetAndWriteToFile(ROWS_ROWCOLVAL_DATASET, COLS_ROWCOLVAL_DATASET, 42, "datasets/rowcolval_dataset.csv");

        /*double[] weights = DataGenerator.generateLinearData("datasets/demo_dataset.csv", ROWS_DEMO_DATASET, COLS_DEMO_DATASET, 5.0);

        final DecimalFormat numberFormat = new DecimalFormat("0.000");
        for (double w : weights)
            System.out.print(numberFormat.format(w) + "\t | ");*/


        // 0.228	 | 0.183	 | -0.191	 | -0.223	 | 0.166	 | 0.403	 | -0.131	 | -0.224	 | -0.036	 | 0.283	 | 0.419	 | -0.064	 | 0.250	 | -0.113	 | -0.323	 |

        // 0.228	 | 0.183	 | -0.191	 |
    }
}
