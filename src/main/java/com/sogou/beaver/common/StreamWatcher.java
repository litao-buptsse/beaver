package com.sogou.beaver.common;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by Tao Li on 2016/6/28.
 */
public class StreamWatcher extends Thread {
  private InputStream stream;
  private StreamProcessor processor;

  public StreamWatcher(InputStream stream, StreamProcessor processor) {
    this.stream = stream;
    this.processor = processor;
  }

  @Override
  public void run() {
    try {
      processor.process(stream);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}