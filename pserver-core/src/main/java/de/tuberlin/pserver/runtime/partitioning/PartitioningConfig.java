package de.tuberlin.pserver.runtime.partitioning;

public class PartitioningConfig {

    public final long matrixNumRows;

    public final long matrixNumCols;

    public PartitioningConfig(long matrixNumRows, long matrixNumCols) {
        this.matrixNumRows = matrixNumRows;
        this.matrixNumCols = matrixNumCols;
    }

    public class Builder {

        private long matrixNumRows;
        private long matrixNumCols;
        public Builder setMatrixNumRows(long matrixNumRows) { this.matrixNumRows = matrixNumRows; return this; }
        public Builder setMatrixNumCols(long matrixNumCols) { this.matrixNumCols = matrixNumCols; return this; }

        PartitioningConfig build() { return new PartitioningConfig(matrixNumRows, matrixNumCols); }

    }

}
