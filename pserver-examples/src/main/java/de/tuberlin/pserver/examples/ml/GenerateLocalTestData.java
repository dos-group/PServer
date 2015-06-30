package de.tuberlin.pserver.examples.ml;

import de.tuberlin.pserver.math.Matrix;
import de.tuberlin.pserver.ml.generators.DataGenerator;

import java.text.DecimalFormat;

public class GenerateLocalTestData {

    public static void main(final String[] args) {

        //DataGenerator.generateDatasetAndWriteToFile(50, 3, 42, "datasets/demo_dataset.csv");

        DataGenerator.generateDatasetAndWriteToFile(5, 1000000, 42, "datasets/sparse_dataset.csv");

        /*double[] weights = DataGenerator.generateLinearData("datasets/demo_dataset.csv", 15, 1000, 5.0);

        final DecimalFormat numberFormat = new DecimalFormat("0.000");
        for (double w : weights)
            System.out.print(numberFormat.format(w) + "\t | ");*/


        // 0.228	 | 0.183	 | -0.191	 | -0.223	 | 0.166	 | 0.403	 | -0.131	 | -0.224	 | -0.036	 | 0.283	 | 0.419	 | -0.064	 | 0.250	 | -0.113	 | -0.323	 |

        // 0.228	 | 0.183	 | -0.191	 |
    }
}
