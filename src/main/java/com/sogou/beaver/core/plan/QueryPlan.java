package com.sogou.beaver.core.plan;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Created by Tao Li on 2016/6/1.
 */
public class QueryPlan {
  private String type;
  private String query;

  public QueryPlan() {
  }

  public QueryPlan(String type, String query) {
    this.type = type;
    this.query = query;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public static QueryPlan fromJson(String json) throws IOException {
    return new ObjectMapper().readValue(json.getBytes(), QueryPlan.class);
  }

  public String toJson() throws JsonProcessingException {
    return new ObjectMapper().writeValueAsString(this);
  }
}
