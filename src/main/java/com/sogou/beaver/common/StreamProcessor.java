package com.sogou.beaver.common;

/**
 * Created by Tao Li on 2016/6/28.
 */

import java.io.IOException;
import java.io.InputStream;

public interface StreamProcessor {
  void process(InputStream stream) throws IOException;
}
