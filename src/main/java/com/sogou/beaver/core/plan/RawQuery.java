package com.sogou.beaver.core.plan;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Tao Li on 6/19/16.
 */
public class RawQuery implements Query {
  private String engine;
  private String sql;
  private Map<String, String> info;

  public RawQuery() {
  }

  public RawQuery(String engine, String sql, Map<String, String> info) {
    this.engine = engine;
    this.sql = sql;
    this.info = info;
  }

  public String getEngine() {
    return engine;
  }

  public void setEngine(String engine) {
    this.engine = engine;
  }

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  public Map<String, String> getInfo() {
    return info;
  }

  public void setInfo(Map<String, String> info) {
    this.info = info;
  }

  @Override
  public String parseEngine() {
    return engine;
  }

  @Override
  public String parseSQL() {
    return sql;
  }

  @Override
  public Map<String, String> parseInfo() {
    return info != null ? info : new HashMap<>();
  }
}