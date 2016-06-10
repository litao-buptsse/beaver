package com.sogou.beaver.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Created by Tao Li on 6/10/16.
 */
public class JobResult {
  private String[] headers;
  private List<String[]> values;

  public JobResult() {
  }

  public JobResult(String[] headers, List<String[]> values) {
    this.headers = headers;
    this.values = values;
  }

  @JsonProperty
  public String[] getHeaders() {
    return headers;
  }

  public void setHeaders(String[] headers) {
    this.headers = headers;
  }

  @JsonProperty
  public List<String[]> getValues() {
    return values;
  }

  public void setValues(List<String[]> values) {
    this.values = values;
  }
}
