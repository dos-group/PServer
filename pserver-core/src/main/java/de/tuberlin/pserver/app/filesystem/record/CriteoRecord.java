package de.tuberlin.pserver.app.filesystem.record;


import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CriteoRecord implements IRecord {

    private static final Logger LOG = LoggerFactory.getLogger(CriteoRecord.class);

    private final double[] values;

    public CriteoRecord(double[] values) {
        this.values = values;
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public double get(int i) {
        return values[i];
    }

    public static CriteoRecord parse(String line, String delimiter, int[] fields) {
        double[] values = new double[fields.length];
        String[] parts = line.split(delimiter);
        if(parts.length == 1) {
            LOG.warn("Delimiter '{}' was not found in line '{}'", delimiter, line);
        }
        // checking fields
        List<Integer> fieldsList = Arrays.asList(ArrayUtils.toObject(fields));
        if(Collections.min(fieldsList) < 0) {
            throw new IllegalArgumentException("Index too small");
        }
        if(Collections.max(fieldsList) > parts.length) {
            throw new IllegalArgumentException("Index too large");
        }
        for(int i = 0; i < fields.length; i++) {
            values[i] = Double.parseDouble(parts[fields[i]]);
        }
        return new CriteoRecord(values);
    }
}
