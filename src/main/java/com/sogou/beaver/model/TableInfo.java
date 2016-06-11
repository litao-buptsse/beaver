package com.sogou.beaver.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by Tao Li on 6/11/16.
 */
public class TableInfo {
  private long id;
  private String name;
  private String description;
  private String frequency;

  public TableInfo(long id, String name, String description, String frequency) {
    this.id = id;
    this.name = name;
    this.description = description;
    this.frequency = frequency;
  }

  @JsonProperty
  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  @JsonProperty
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @JsonProperty
  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  @JsonProperty
  public String getFrequency() {
    return frequency;
  }

  public void setFrequency(String frequency) {
    this.frequency = frequency;
  }
}
