package de.tuberlin.pserver.ml;

import de.tuberlin.pserver.client.PServerExecutor;
import de.tuberlin.pserver.compiler.Program;
import de.tuberlin.pserver.crdt.matrix.own.DenseMatrix;
import de.tuberlin.pserver.dsl.unit.annotations.Unit;
import de.tuberlin.pserver.dsl.unit.controlflow.lifecycle.Lifecycle;

import java.io.*;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class KMeansCRDT extends Program {
    DenseMatrix<Double> matrix;
    DenseMatrix<Float> centroids;
    int k = 6;

    @Unit(at = "0")
    public void blub(Lifecycle lifecycle) {
        lifecycle.preProcess(() -> {
            float[][] m = readFile(new File("datasets/stripes2.csv"));
            matrix = new DenseMatrix<>(m.length, 2, "matrix", 2, programContext);

            for(int i = 0; i < m.length; i++) {
                for (int n = 0; n < m[i].length; n++) {
                    matrix.set(i, n, m[i][n]);
                }
            }

            matrix.finish();


        }).process(() -> {

            getStartCentroids(k);

            System.out.println();
            System.out.println("Centroids:");
            for (int i = 0; i < centroids.getRows(); i++) {
                System.out.println("(" + centroids.getRow(i)[0] + ", " + centroids.getRow(i)[1] + ")");
            }
            System.out.println();

            for(int n = 0; n < 20; n++) {
                int[] clusters = cluster();

                float[][] newCentroids = getNewCentroids(clusters);

                System.out.println();
                System.out.println("New Centroids:");
                for (int i = 0; i < newCentroids.length; i++) {
                    System.out.println("(" + newCentroids[i][0] + ", " + newCentroids[i][1] + ")");
                }
                System.out.println();

                for(int i = 0; i < newCentroids.length; i++) {
                    centroids.setRow(new float[]{newCentroids[i][0], newCentroids[i][1]}, i);
                }
            }


        });
    }

    @Unit(at = "1")
    public void blub2(Lifecycle lifecycle) {
        lifecycle.preProcess(() -> {
            float[][] m = readFile(new File("datasets/stripes2.csv"));
            matrix = new DenseMatrix<>(m.length, 2, "matrix", 2, programContext);
        }).process(() -> {

            matrix.finish();

            System.out.println("***" + matrix.getRow(3).length);
            for(float f : matrix.getRow(3)) {
                System.out.println(f);
            }


        });
    }

    // Todo: this can be heavily optimized
    private DenseMatrix<Float> getStartCentroids(int k) {
        centroids = new DenseMatrix<>(k, 2, "cent", 2, programContext);
        int assigned = 0;
        // First centroid chosen randomly
        centroids.setRow(matrix.getRow(0), assigned);
        assigned ++;

        // continue choosing centroids that are the furthest distance from current centroids
        while(assigned < k) {
            int furthestRow = 0;
            double furthestDistance = 0;
            for (int i = 0; i < matrix.getRows(); i++) {

                double distanceSum = 0;
                for (int n = 0; n < assigned; n++) {
                    distanceSum += euclideanDistance(centroids.get(n, 0), centroids.get(n, 1),
                            matrix.get(i, 0), matrix.get(i, 1));
                }

                if (distanceSum > furthestDistance) {
                    furthestDistance = distanceSum;
                    furthestRow = i;
                }
            }
            centroids.setRow(matrix.getRow(furthestRow), assigned);
            assigned++;
        }

        return centroids;
    }

    private int[] cluster() {
        int[] clusters = new int[(int)matrix.getRows()];



        for(int i = 0; i < matrix.getRows(); i++) {
            int closestCentroid = 0;
            double closestDistance = Double.MAX_VALUE;

            for(int n = 0; n < centroids.getRows(); n++) {
                double distance = euclideanDistance(centroids.getRow(n)[0], centroids.getRow(n)[1],
                        matrix.getRow(i)[0], matrix.getRow(i)[1]);

                if(distance < closestDistance) {
                    closestCentroid = n;
                    closestDistance = distance;
                }
            }
            clusters[i] = closestCentroid;
        }
        return clusters;
    }

    private float[][] getNewCentroids(int[] clusters) {
        float[][] newCentroids = new float[(int)centroids.getRows()][2];
        long[][] sums = new long[k][2];
        int[] count = new int[k];

        for(int i = 0; i < k; i++) {
            Arrays.fill(sums[i], 0);
        }
        Arrays.fill(count, 0);

        for(int i = 0; i < clusters.length; i++) {
            int cluster = (int)clusters[i];
            sums[cluster][0] += matrix.getRow(i)[0];
            sums[cluster][1] += matrix.getRow(i)[1];
            count[cluster] = count[cluster]+1;
        }

        for(int i = 0; i < sums.length; i++) {
            if(count[i] != 0) {
                newCentroids[i][0] = sums[i][0] / (float)count[i];
                newCentroids[i][1] = sums[i][1] / (float)count[i];
            } else {
                newCentroids[i][0] = 0;
                newCentroids[i][1] = 0;
            }
        }

        return newCentroids;

    }

    private double euclideanDistance(double x1, double x2, double y1, double y2) {
        return Math.sqrt(Math.pow(y1 - x1, 2) + Math.pow(y2 - x2, 2));
    }



    private static float[][] readFile(File f) {
        List<Float> leftCol = new LinkedList<>();
        List<Float> rightCol = new LinkedList<>();

        try {
            BufferedReader reader = new BufferedReader(new FileReader(f));
            String line;
            String[] values;
            line = reader.readLine();

            while(line != null) {
                values = line.split(",");
                leftCol.add(Float.valueOf(values[0]));
                rightCol.add(Float.valueOf(values[1]));
                line = reader.readLine();
            }


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        float[][] matrix = new float[leftCol.size()][2];

        for(int i = 0; i < leftCol.size(); i++) {
            matrix[i][0] = leftCol.get(i);
            matrix[i][1] = rightCol.get(i);
        }

        return matrix;

    }

    public static void main(String args[]) {
        System.setProperty("simulation.numNodes", "2");
        PServerExecutor.LOCAL
                .run(KMeansCRDT.class)
                .done();

    }
}
