package com.sogou.beaver.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by Tao Li on 6/11/16.
 */
public class FieldInfo {
  private long id;
  private long tableId;
  private String name;
  private String description;
  private String type;

  public FieldInfo() {
  }

  public FieldInfo(long id, long tableId, String name, String description, String type) {
    this.id = id;
    this.tableId = tableId;
    this.name = name;
    this.description = description;
    this.type = type;
  }

  @JsonProperty
  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  @JsonProperty
  public long getTableId() {
    return tableId;
  }

  public void setTableId(long tableId) {
    this.tableId = tableId;
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
  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }
}
