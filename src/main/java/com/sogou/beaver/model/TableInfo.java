package com.sogou.beaver.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by Tao Li on 6/11/16.
 */
public class TableInfo {
  private long id;
  private long tableId;
  private String name;
  private String database;
  private String tableName;
  private String description;
  private String[] frequencies;
  private String fileFormat;
  private String explodeField;
  private String preFilterSQL;

  public TableInfo(long id, long tableId, String database, String tableName, String description,
                   String[] frequencies, String fileFormat, String explodeField, String preFilterSQL) {
    this.id = id;
    this.tableId = tableId;
    this.database = database;
    this.tableName = tableName;
    this.name = database + "." + tableName +
        (explodeField != null && !explodeField.equals("") ? ":" + explodeField : "");
    this.description = description;
    this.frequencies = frequencies;
    this.fileFormat = fileFormat;
    this.explodeField = explodeField != null && explodeField.equals("") ? null : explodeField;
    this.preFilterSQL = preFilterSQL != null && preFilterSQL.equals("") ? null : preFilterSQL;
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

  public String[] getFrequencies() {
    return frequencies;
  }

  public void setFrequencies(String[] frequencies) {
    this.frequencies = frequencies;
  }

  @JsonProperty
  public String getFileFormat() {
    return fileFormat;
  }

  public void setFileFormat(String fileFormat) {
    this.fileFormat = fileFormat;
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
