package com.sogou.beaver.core.plan;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Created by Tao Li on 2016/6/1.
 */
public class ExecutionPlan {
  private String engine;
  private String sql;

  public ExecutionPlan() {
  }

  public ExecutionPlan(String engine, String sql) {
    this.engine = engine;
    this.sql = sql;
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

  public static ExecutionPlan fromJson(String json) throws IOException {
    return new ObjectMapper().readValue(json.getBytes(), ExecutionPlan.class);
  }

  public String toJson() throws JsonProcessingException {
    return new ObjectMapper().writeValueAsString(this);
  }
}
