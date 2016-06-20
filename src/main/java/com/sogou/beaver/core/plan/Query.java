package com.sogou.beaver.core.plan;

import java.util.Map;

/**
 * Created by Tao Li on 6/19/16.
 */
public interface Query {
  String parseEngine();

  String parseSQL();

  Map<String, String> parseInfo();
}
