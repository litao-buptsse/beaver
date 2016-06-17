package com.sogou.beaver.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by Tao Li on 2016/6/17.
 */
public class EnumInfo {
  private long id;
  private long fieldId;
  private String value;
  private String description;

  public EnumInfo() {
  }

  public EnumInfo(long id, long fieldId, String value, String description) {
    this.id = id;
    this.fieldId = fieldId;
    this.value = value;
    this.description = description;
  }

  @JsonProperty
  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  @JsonProperty
  public long getFieldId() {
    return fieldId;
  }

  public void setFieldId(long fieldId) {
    this.fieldId = fieldId;
  }

  @JsonProperty
  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  @JsonProperty
  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }
}
