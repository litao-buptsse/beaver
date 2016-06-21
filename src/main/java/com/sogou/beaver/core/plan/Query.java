package com.sogou.beaver.core.plan;

import java.util.Map;

/**
 * Created by Tao Li on 6/19/16.
 */
public interface Query {
  String parseEngine() throws ParseException;

  String parseSQL() throws ParseException;

  Map<String, String> parseInfo() throws ParseException;
}
