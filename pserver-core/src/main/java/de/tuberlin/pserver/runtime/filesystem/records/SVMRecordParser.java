package de.tuberlin.pserver.runtime.filesystem.records;

import de.tuberlin.pserver.runtime.filesystem.AbstractFileIterationContext;

public final class SVMRecordParser {

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------

    private final AbstractFileIterationContext iteratorContext;

    private final Record reusedRecord;

    private final char[] parseBuffer;

    private final StringBuilder stringBuilder;

    private int bufferIndex = 0;

    private char currentChar;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public SVMRecordParser(AbstractFileIterationContext iteratorContext) {
        this.iteratorContext    = iteratorContext;
        this.reusedRecord       = new Record();
        this.parseBuffer        = new char[256];
        this.stringBuilder      = new StringBuilder();
    }

    // ---------------------------------------------------
    // Public Method.
    // ---------------------------------------------------

    public Record parseNextRow(int row) {
        reusedRecord.row = row;
        try {
            // --------------------------------------
            // PARSE LABEL
            // --------------------------------------
            if (!readNextByte())
                throw new IllegalStateException();
            while ((currentChar == '-' || currentChar == '.' || currentChar == 'E' || Character.isDigit(currentChar))) {
                parseBuffer[bufferIndex++] = currentChar;
                if (!readNextByte())
                    break;
            }
            stringBuilder.append(parseBuffer, 0, bufferIndex);
            reusedRecord.label = Float.parseFloat(stringBuilder.toString());
            stringBuilder.setLength(0);
            bufferIndex = 0;
            // --------------------------------------
            // PARSE ENTRY
            // --------------------------------------
            while (currentChar != '\n') {
                // ---------------
                // PARSE INDEX
                // ---------------
                if (currentChar != ' ')
                    throw new IllegalStateException();
                if (!readNextByte())
                    throw new IllegalStateException();
                while ((currentChar == '-' || currentChar == '.' || currentChar == 'E' || Character.isDigit(currentChar))) {
                    parseBuffer[bufferIndex++] = currentChar;
                    if (!readNextByte())
                        break;
                }
                stringBuilder.append(parseBuffer, 0, bufferIndex);
                int index = Integer.parseInt(stringBuilder.toString());
                stringBuilder.setLength(0);
                bufferIndex = 0;
                // ---------------
                // PARSE VALUE
                // ---------------
                if (currentChar != ':')
                    throw new IllegalStateException();
                if (!readNextByte())
                    throw new IllegalStateException();
                while ((currentChar == '-' || currentChar == '.' || currentChar == 'E' || Character.isDigit(currentChar))) {
                    parseBuffer[bufferIndex++] = currentChar;
                    if (!readNextByte())
                        break;
                }
                stringBuilder.append(parseBuffer, 0, bufferIndex);
                index--;
                reusedRecord.entries.put(index, Float.parseFloat(stringBuilder.toString()));
                stringBuilder.setLength(0);
                bufferIndex = 0;
            }
        } catch(Throwable t) {
            throw new IllegalStateException(t);
        }
        return reusedRecord;
    }

    // ---------------------------------------------------
    // Private Method.
    // ---------------------------------------------------

    private boolean readNextByte() throws Exception {
        int i = iteratorContext.readNext();
        currentChar = (char) i;
        return !(i == -1);
    }
}
