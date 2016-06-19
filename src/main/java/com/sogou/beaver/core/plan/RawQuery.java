package com.sogou.beaver.core.plan;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sogou.beaver.db.JDBCConnectionPool;

import java.io.IOException;

/**
 * Created by Tao Li on 6/19/16.
 */
public class RawQuery implements Query {
  private String engine;
  private String sql;

  public RawQuery() {
  }

  public RawQuery(String engine, String sql) {
    this.engine = engine;
    this.sql = sql;
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

  public static RawQuery fromJson(String json) throws IOException {
    return new ObjectMapper().readValue(json.getBytes(), RawQuery.class);
  }

  public String toJson() throws JsonProcessingException {
    return new ObjectMapper().writeValueAsString(this);
  }

  @Override
  public String parseEngine(JDBCConnectionPool pool) {
    return engine;
  }

  @Override
  public String parseSQL(JDBCConnectionPool pool) {
    return sql;
  }
}