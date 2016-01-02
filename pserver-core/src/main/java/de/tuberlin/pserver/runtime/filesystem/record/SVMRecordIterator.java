package de.tuberlin.pserver.runtime.filesystem.record;

import com.google.common.base.Preconditions;
import de.tuberlin.pserver.math.tuples.Tuple2;

import java.io.*;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Created by Morgan K. Geldenhuys on 17.12.15.
 */
public class SVMRecordIterator<V extends Number> implements RecordIterator {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    private class SVMParser {

        private char separator;
        private char delimiter;

        public SVMParser(char separator, char delimiter) {
            this.separator = separator;
            this.delimiter = delimiter;
        }

        public Tuple2<Integer, Map<Integer, V>> parse(String line) {
            int target;
            double[] attributes;
            return null;
        }

    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    private Stream<String> lines;
    private long currentLine;
    private SVMParser svmParser;
    private int[] projection;
    private SVMRecord reusable;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SVMRecordIterator(InputStream inputStream, char separator, char delimiter, int[] projection) {
        this.lines = (new BufferedReader(new InputStreamReader(Preconditions.checkNotNull(inputStream)))).lines();
        this.currentLine = 1;
        this.svmParser = new SVMParser(separator, delimiter);
        this.projection = projection;
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    @Override
    public boolean hasNext() {
        return this.lines.skip(--this.currentLine).findFirst().isPresent();
    }

    public SVMRecord next() {
        String line = this.lines.skip(--this.currentLine).findFirst().get();
        return this.reusable.set(this.svmParser.parse(line));
    }

    @Override
    public SVMRecord next(long lineNumber) {
        String line = this.lines.skip(--lineNumber).findFirst().get();
        this.currentLine = lineNumber;
        return this.reusable.set(this.svmParser.parse(line));
    }

}
