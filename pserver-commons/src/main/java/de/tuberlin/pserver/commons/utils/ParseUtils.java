package de.tuberlin.pserver.commons.utils;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class ParseUtils {

    public static int[] parseNodeRanges(final String rangeDefinition) {

        if (rangeDefinition.contains("-")) { // interval definition

            final StringTokenizer tokenizer = new StringTokenizer(Preconditions.checkNotNull(rangeDefinition), "-");
            final List<Integer> vals = new ArrayList<>();
            final int fromNodeID = Integer.valueOf(tokenizer.nextToken().replaceAll("\\s+", ""));
            final int toNodeID = Integer.valueOf(tokenizer.nextToken().replaceAll("\\s+", ""));

            if (tokenizer.hasMoreTokens())
                throw new IllegalStateException();

            for (int i = fromNodeID; i <= toNodeID; ++i) vals.add(i);

            return Ints.toArray(vals);

        } else { // comma separated definition

            final StringTokenizer tokenizer = new StringTokenizer(Preconditions.checkNotNull(rangeDefinition), ",");
            final List<Integer> vals = new ArrayList<>();

            while (tokenizer.hasMoreTokens())
                vals.add(Integer.valueOf(tokenizer.nextToken().replaceAll("\\s+", "")));

            return Ints.toArray(vals);
        }
    }

    public static List<String> parseStateList(final String stateNames) {
        final List<String> stateList = new ArrayList<>();
        final StringTokenizer tokenizer = new StringTokenizer(Preconditions.checkNotNull(stateNames), ",");
        while (tokenizer.hasMoreTokens())
            stateList.add(tokenizer.nextToken().replaceAll("\\s+", ""));
        return stateList;
    }
}
