package de.tuberlin.pserver;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SVMConverter {

    private static class Attribute {
        long col;
        double val;

        public Attribute(long col, double val) {
            this.col = col;
            this.val = val;
        }

        @Override
        public String toString() {
            return " " + col + ":" + val;
        }
    }

    public static void main(String[] args) throws IOException {

        if (args.length != 3)
            throw new IllegalArgumentException("Usage > svmconverter input_train_file input_test_file output_file");

        Stream<String> trainLines = null, testLines = null;
        Path output = Paths.get(args[2]);
        try {
            trainLines = Files.lines(Paths.get(args[0]));
            testLines = Files.lines(Paths.get(args[1]));
            Files.deleteIfExists(output);
        }
        catch(IOException e) {
            e.printStackTrace();
        }

        Map<Long, Integer> testData =
            testLines
                .map(s -> {
                    String[] tokens = s.split(",");
                    if (tokens.length != 3)
                        throw new RuntimeException("Invalid file format");
                    return tokens;
                })
                .collect(Collectors.toMap(arr -> Long.parseLong(arr[0]), arr -> Integer.parseInt(arr[2])));

        trainLines
            .map(s -> {
                String[] tokens = s.split(",");
                if (tokens.length != 3)
                    throw new RuntimeException("Invalid file format");
                Attribute attribute =
                    new Attribute(Long.parseLong(tokens[1]) + 1, Double.parseDouble(tokens[2]));
                return new AbstractMap.SimpleEntry<>(Long.parseLong(tokens[0]), attribute);
            })
            .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, Collectors.toList())))
            .forEach((k, v) -> {
                Optional label = Optional.of(testData.get(k));
                StringBuilder sb = new StringBuilder(label.isPresent() ? "" + label.get() : "0");
                for (Attribute attribute : v)
                    sb.append(attribute);
                sb.append("\n");
                byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
                try {
                    Files.write(output, bytes, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                } catch(IOException e) {
                    e.printStackTrace();
                }
            });
    }

}
