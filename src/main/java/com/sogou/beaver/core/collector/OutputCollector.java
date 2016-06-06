package com.sogou.beaver.core.collector;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Created by Tao Li on 2016/6/6.
 */
public interface OutputCollector extends Closeable {
  void collect(List<String> values) throws IOException;
}
