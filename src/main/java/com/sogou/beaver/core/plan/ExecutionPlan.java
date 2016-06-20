package com.sogou.beaver.core.plan;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Tao Li on 2016/6/1.
 */
public class ExecutionPlan {
  private String engine;
  private String sql;
  private Map<String, String> info;

  public ExecutionPlan() {
  }

  public ExecutionPlan(String engine, String sql, Map<String, String> info) {
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

  public static ExecutionPlan fromJson(String json) throws IOException {
    return new ObjectMapper().readValue(json.getBytes(), ExecutionPlan.class);
  }

  public String toJson() throws JsonProcessingException {
    return new ObjectMapper().writeValueAsString(this);
  }
}
