package de.tuberlin.pserver.ml.playground.mahout.examples.adultincome;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.AbstractIterator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

/** Parses comma separated data as Adults information  */
public class AdultDataParser implements Iterable<AdultData> {

  private final Splitter onSemi = Splitter.on(",").trimResults(CharMatcher.anyOf("\" ;"));
  private String resourceName;

  public AdultDataParser(String resourceName) throws IOException {
    this.resourceName = resourceName;
  }

  @Override
  public Iterator<AdultData> iterator() {
    try {
      return new AbstractIterator<AdultData>() {
        BufferedReader input =
            new BufferedReader(new FileReader(resourceName));
        Iterable<String> fieldNames = onSemi.split(input.readLine());

          @Override
          protected AdultData computeNext() {
            try {
              String line = input.readLine();
              if (line == null) {
                return endOfData();
              }

              return new AdultData(fieldNames, onSemi.split(line));
            } catch (IOException e) {
              throw new RuntimeException("Error reading data", e);
            }
          }
        };
      } catch (IOException e) {
        throw new RuntimeException("Error reading data", e);
      }
  }
}
