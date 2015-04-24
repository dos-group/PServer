package de.tuberlin.pserver.ml.generators;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;


public class SyntheticDataGenerator {

    private static final String NEW_LINE_SEPARATOR = "\n";
    private static final Random random = new Random();


    public static void linearFunctionSyntheticGenerator(int row, int col) throws IOException {

        final double[][] syntheticData = new double[row][col + 1];
        final double[] syntheticParameter = new double[col];

        for (int x = 0; x < syntheticParameter.length; )
            syntheticParameter[x++] = random.nextDouble();

        for (int i = 0; i < row; i++) {
            double label = 0;
            for (int j = 0; j < col; j++) {
                syntheticData[i][j] = random.nextDouble();
                label += (syntheticData[i][j] * syntheticParameter[j]);
            }
            syntheticData[i][col] = label + Math.abs(random.nextGaussian());
        }

        CSVParametersWriter(syntheticParameter, SyntheticType.LINEAR);
        CSVDataWriter(syntheticData, SyntheticType.LINEAR);

    }


    public static void polynomialFunctionSyntheticGenerator(int row, int col) throws IOException{

        final double[][] syntheticData = new double[row][col + 1];
        final double[] syntheticParameter = new double[col];

        for (int x = 0; x < syntheticParameter.length; )
            syntheticParameter[x++] = random.nextDouble();

        for (int i = 0; i < row; i++) {
            double label = 0;
            double randTmp = ((double) Math.round(random.nextDouble() * 100) / 100);

            for (int j = 0; j < col; j++) {
                syntheticData[i][j] = Math.pow(randTmp, j);
                label += (syntheticData[i][j] * syntheticParameter[j]);
            }
            syntheticData[i][col] = label + Math.abs(random.nextGaussian());
        }

        CSVParametersWriter(syntheticParameter, SyntheticType.POLYNOMIAL);
        CSVDataWriter(syntheticData, SyntheticType.POLYNOMIAL);
    }

    public static void sineFunctionSyntheticGenerator(int row, int col) throws IOException{

        final double[][] syntheticData = new double[row][col + 1];
        final double[] syntheticParameter = new double[col];

        for (int x = 0; x < syntheticParameter.length; )
            syntheticParameter[x++] = random.nextDouble();

        for (int i = 0; i < row; i++) {
            double label = 0;

            for (int j = 0; j < col; j++) {
                syntheticData[i][j] = random.nextDouble();
                label += (syntheticData[i][j] * syntheticParameter[j]);
            }
            syntheticData[i][col] = label + Math.abs(random.nextGaussian());
        }

        CSVParametersWriter(syntheticParameter, SyntheticType.POLYNOMIAL);
        CSVDataWriter(syntheticData, SyntheticType.POLYNOMIAL);
    }



    public static void CSVDataWriter(double[][] data, SyntheticType type) throws IOException {

        File file = null;
        FileWriter fileWriter = null;

        switch (type) {
            case LINEAR:
                file = new File("../SGD/ML_Data/SyntheticData/linear/data.data");
                if (file.getParentFile().exists())
                    file.createNewFile();
                else {
                    file.getParentFile().mkdir();
                    file.createNewFile();
                }
                break;
            case POLYNOMIAL:
                file = new File("../SGD/ML_Data/SyntheticData/polynomial/data.data");
                if (file.getParentFile().exists())
                    file.createNewFile();
                else {
                    file.getParentFile().mkdir();
                    file.createNewFile();
                }
                break;
            default:
                break;
        }

        try {

            fileWriter = new FileWriter(file);
            CSVFormat format = CSVFormat.DEFAULT.withRecordSeparator(NEW_LINE_SEPARATOR);
            CSVPrinter csvPrinter = new CSVPrinter(fileWriter, format);

            for (int i = 0; i < data.length; i++) {
                ArrayList<Double> record = new ArrayList<>();
                for (double x : data[i]) {
                    record.add(x);
                }
                csvPrinter.printRecord(record);
                record = null;
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } finally {
            fileWriter.flush();
            fileWriter.close();
        }
    }



    public static void CSVParametersWriter(double[] data, SyntheticType type) throws IOException {

        File file = null;
        FileWriter fileWriter = null;

        switch (type) {
            case LINEAR:
                file = new File("../SGD/ML_Data/SyntheticData/linear/parameters.data");
                if (file.getParentFile().exists())
                    file.createNewFile();
                else {
                    file.getParentFile().mkdir();
                    file.createNewFile();
                }
                break;
            case POLYNOMIAL:
                file = new File("../SGD/ML_Data/SyntheticData/polynomial/parameters.data");
                if (file.getParentFile().exists())
                    file.createNewFile();
                else {
                    file.getParentFile().mkdir();
                    file.createNewFile();
                }
                break;
            default:
                break;
        }

        try {
            fileWriter = new FileWriter(file);
            CSVFormat format = CSVFormat.DEFAULT.withRecordSeparator(NEW_LINE_SEPARATOR);
            CSVPrinter csvPrinter = new CSVPrinter(fileWriter, format);

            for (int i = 0; i < data.length; i++) {
                ArrayList<Double> record = new ArrayList<>();
                record.add(data[i]);
                csvPrinter.printRecord(record);
                record = null;
            }

        } catch (IOException e) {
            System.out.println(e.getMessage());
        } finally {
            fileWriter.flush();
            fileWriter.close();
        }
    }

    public enum SyntheticType{
        LINEAR,POLYNOMIAL,SIN;
    }
}
