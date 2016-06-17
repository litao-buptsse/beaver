package com.sogou.beaver.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Created by Tao Li on 6/11/16.
 */
public class FieldInfo {
  private long id;
  private long tableId;
  private String name;
  private String description;
  private String comment;
  private String dataType;
  private String fieldType;
  private boolean isEnum;

  public static class EnumValue {
    private String value;
    private String description;

    public EnumValue() {
    }

    public EnumValue(String value, String description) {
      this.value = value;
      this.description = description;
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

  public FieldInfo() {
  }

  public FieldInfo(long id, long tableId, String name, String description, String comment,
                   String dataType, String fieldType, boolean isEnum) {
    this.id = id;
    this.tableId = tableId;
    this.name = name;
    this.description = description;
    this.comment = comment;
    this.dataType = dataType;
    this.fieldType = fieldType;
    this.isEnum = isEnum;
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
  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  @JsonProperty
  public String getDataType() {
    return dataType;
  }

  public void setDataType(String dataType) {
    this.dataType = dataType;
  }

  @JsonProperty
  public String getFieldType() {
    return fieldType;
  }

  public void setFieldType(String fieldType) {
    this.fieldType = fieldType;
  }

  @JsonProperty
  public boolean isEnum() {
    return isEnum;
  }

  public void setEnum(boolean anEnum) {
    isEnum = anEnum;
  }
}
