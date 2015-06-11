package de.tuberlin.pserver.examples.graphs;


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class GenerateAdjMatrixDiGraph {

    public static void generateAdjMatrixDiGraphAndWriteToFile(final int V,
                                                              final int E,
                                                              final long seed,
                                                              final String fileName) {
        final Random rand = new Random();
        rand.setSeed(seed);
        final AdjMatrixDigraph g = new AdjMatrixDigraph(V, E);
        FileWriter writer   = null;
        CSVPrinter printer  = null;
        final CSVFormat csvFileFormat = CSVFormat.DEFAULT.withRecordSeparator("\n");

        try {
            writer = new FileWriter(fileName + ".dat");
            printer = new CSVPrinter(writer, csvFileFormat);
            List<Double> values = new ArrayList<>();
            for (int i = 0; i < g.V(); ++i) {
                for (int j = 0; j < g.V(); ++j) {
                    values.add(g.matrix.get(i, j));
                }
                printer.printRecord(values);
                values.clear();
            }
            writer.flush();
            writer.close();
            printer.close();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static void main(final String[] args) {
        generateAdjMatrixDiGraphAndWriteToFile(1000, 1000 * 500, 42, "datasets/graph");
    }
}
