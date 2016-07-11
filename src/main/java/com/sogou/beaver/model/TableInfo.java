package com.sogou.beaver.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by Tao Li on 6/11/16.
 */
public class TableInfo {
  private long id;
  private String name;
  private String database;
  private String tableName;
  private String description;
  private String frequency;
  private String fileFormat;
  private boolean isExplode;
  private String explodeField;
  private String preFilterSQL;

  public TableInfo(long id, String database, String tableName, String description,
                   String frequency, String fileFormat, boolean isExplode, String explodeField,
                   String preFilterSQL) {
    this.id = id;
    this.database = database;
    this.tableName = tableName;
    this.name = database + "." + tableName + (isExplode ? ":" + explodeField : "");
    this.description = description;
    this.frequency = frequency;
    this.fileFormat = fileFormat;
    this.isExplode = isExplode;
    this.explodeField = explodeField;
    this.preFilterSQL = preFilterSQL;
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
  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  @JsonProperty
  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
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

  @JsonProperty
  public String getFileFormat() {
    return fileFormat;
  }

  public void setFileFormat(String fileFormat) {
    this.fileFormat = fileFormat;
  }

  @JsonProperty
  public boolean getIsExplode() {
    return isExplode;
  }

  public void setExplode(boolean explode) {
    isExplode = explode;
  }

  @JsonProperty
  public String getExplodeField() {
    return explodeField;
  }

  public void setExplodeField(String explodeField) {
    this.explodeField = explodeField;
  }

  @JsonProperty
  public String getPreFilterSQL() {
    return preFilterSQL;
  }

  public void setPreFilterSQL(String preFilterSQL) {
    this.preFilterSQL = preFilterSQL;
  }
}
