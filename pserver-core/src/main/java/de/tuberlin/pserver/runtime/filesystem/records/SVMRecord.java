package de.tuberlin.pserver.runtime.filesystem.records;

import de.tuberlin.pserver.types.typeinfo.properties.FileFormat;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.map.TLongFloatMap;
import gnu.trove.map.hash.TLongFloatHashMap;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.text.StrTokenizer;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class SVMRecord implements Record {

    // ---------------------------------------------------
    // Inner Classes.
    // ---------------------------------------------------

    static class SVMParser {

        private static FileFormat fileFormat = FileFormat.SVM_FORMAT;

        private static StrTokenizer tokenizer = new StrTokenizer();

        private static List<Long> projectionList;

        // ---------------------------------------------------

        public static void setFileFormat(FileFormat fileFormat) { SVMParser.fileFormat = fileFormat; }

        public static void setProjection(Optional<long[]> projection) {
            if (projection.isPresent())
                projectionList = Arrays.asList(ArrayUtils.toObject(projection.get()));
        }

        protected static float parse(String line, TLongFloatMap attributes) {
            float label;
            tokenizer.setDelimiterString(FileFormat.SVM_FORMAT.getDelimiter());
            tokenizer.reset(line);
            if (fileFormat.getValueType() == FileFormat.ValueType.FLOAT)
                label = Float.parseFloat(tokenizer.nextToken());
            else
                throw new UnsupportedOperationException();
            while(tokenizer.hasNext()) {
                String column = tokenizer.nextToken();
                long index = Long.parseLong(column.substring(0, column.indexOf(":")));
                // index is decremented because LIBSVM format starts on col index 1 and we need to load this into a [0,0] matrix
                index--;
                if (projectionList == null || projectionList.contains(index)) {
                    if (fileFormat.getValueType() == FileFormat.ValueType.FLOAT)
                        attributes.put(index, Float.parseFloat(column.substring(column.indexOf(":") + 1)));
                    else
                        throw new UnsupportedOperationException();
                }
            }
            return label;
        }
    }

    // ---------------------------------------------------
    // State.
    // ---------------------------------------------------

    private long row;

    private float label;

    private TLongFloatIterator attrIterator;

    private TLongFloatMap attributes = new TLongFloatHashMap();

    private RecordEntry32F reusableEntry = new RecordEntry32F(-1, -1, Float.NaN);

    // ---------------------------------------------------
    // Constructor.
    // ---------------------------------------------------

    public SVMRecord(Optional<long[]> projection) {
        SVMParser.setProjection(projection);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    public long getRow() { return this.row; }

    public void setLabel(float label) { this.label = label; }

    public float getLabel() { return this.label; }

    @Override public int size() { return this.attributes.size(); }

    @Override public boolean hasNext() { return attrIterator.hasNext(); }

    // ---------------------------------------------------

    @Override
    public RecordEntry32F next() {
        attrIterator.advance();
        return reusableEntry.set(this.row, attrIterator.key(), attrIterator.value());
    }

    @Override
    public SVMRecord set(long row, String line) {
        this.row = row;
        this.attributes.clear();
        this.label = SVMParser.parse(line, attributes);
        this.attrIterator = attributes.iterator();
        return this;
    }
}