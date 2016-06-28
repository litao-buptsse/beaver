package com.sogou.beaver.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Tao Li on 2016/6/28.
 */
public class StreamCollector implements StreamProcessor {
  private List<String> output = new ArrayList<>();

  @Override
  public void process(InputStream stream) throws IOException {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
      String line;
      while ((line = reader.readLine()) != null) {
        output.add(line);
      }
    }
  }

  public List<String> getOutput() {
    return output;
  }
}
