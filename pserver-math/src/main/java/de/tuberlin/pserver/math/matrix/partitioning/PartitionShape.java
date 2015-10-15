package de.tuberlin.pserver.math.matrix.partitioning;

import org.apache.commons.lang3.builder.EqualsBuilder;

public final class PartitionShape {

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public final long rows;

    public final long cols;

    public final long rowOffset;

    public final long colOffset;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public PartitionShape(long rows, long cols) {
        this(rows, cols, 0, 0);
    }

    public PartitionShape(long rows, long cols, long rowOffset, long colOffset) {
        this.rows = rows;
        this.cols = cols;
        this.rowOffset = rowOffset;
        this.colOffset = colOffset;
    }

    public PartitionShape create(long row, long col) {
        return new PartitionShape(row, col);
    }

    // ---------------------------------------------------
    // Public Methods.
    // ---------------------------------------------------


    public boolean contains(long row, long col) {
        return row < rows && col < cols;
    }

    public PartitionShape intersect(PartitionShape other) {
        long maxRowStart = Math.max(rowOffset, other.rowOffset);
        long minRowEnd   = Math.min(rowOffset + rows, other.rowOffset + other.rows);
        long maxColStart = Math.max(colOffset, other.colOffset);
        long minColEnd   = Math.min(colOffset + cols, other.colOffset + other.cols);
        if(maxRowStart <= minRowEnd && maxColStart <= minColEnd) {
            return new PartitionShape(minRowEnd - maxRowStart, minColEnd - maxColStart, maxRowStart, maxColStart);
        }
        return null;
    }

    @Override public String toString() { return "PartitionShape ("+rows+"+"+rowOffset+","+cols+"+"+colOffset+")"; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        PartitionShape that = (PartitionShape) o;

        return new EqualsBuilder()
                .append(rows, that.rows)
                .append(cols, that.cols)
                .append(rowOffset, that.rowOffset)
                .append(colOffset, that.colOffset)
                .isEquals();
    }
}