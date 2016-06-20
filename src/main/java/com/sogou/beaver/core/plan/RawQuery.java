package com.sogou.beaver.core.plan;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sogou.beaver.db.JDBCConnectionPool;

import java.io.IOException;
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

  @JsonProperty
  public String getEngine() {
    return engine;
  }

  public void setEngine(String engine) {
    this.engine = engine;
  }

  @JsonProperty
  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  @JsonProperty
  public Map<String, String> getInfo() {
    return info;
  }

  public void setInfo(Map<String, String> info) {
    this.info = info;
  }

  public static RawQuery fromJson(String json) throws IOException {
    return new ObjectMapper().readValue(json.getBytes(), RawQuery.class);
  }

  public String toJson() throws JsonProcessingException {
    return new ObjectMapper().writeValueAsString(this);
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