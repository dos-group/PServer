package de.tuberlin.pserver.runtime.filesystem;

/**
 * Created by Morgan K. Geldenhuys on 18.12.15.
 */
public enum FileFormat {

    DENSE_FORMAT(",", "\n", ValueType.FLOAT),
    SPARSE_FORMAT(",", "\n", ValueType.FLOAT),
    SVM_FORMAT(" \t", "\n", ValueType.FLOAT);

    public enum ValueType {
        FLOAT, DOUBLE
    };

    private String delimiter;
    private String separator;
    private ValueType valueType;

    FileFormat(String delimiter, String separator, ValueType valueType) {
        this.delimiter = delimiter;
        this.separator = separator;
        this.valueType = valueType;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }

    public ValueType getValueType() {
        return this.valueType;
    }

    public void setValueType(ValueType valueType) {
        this.valueType = valueType;
    }

}
