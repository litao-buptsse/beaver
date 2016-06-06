package com.sogou.beaver.core.meta;

/**
 * Created by Tao Li on 2016/6/6.
 */
public class ColumnMeta {
  private String columnName;
  private int columnType;
  private String columnTypeName;

  public ColumnMeta(String columnName, int columnType, String columnTypeName) {
    this.columnName = columnName;
    this.columnType = columnType;
    this.columnTypeName = columnTypeName;
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public int getColumnType() {
    return columnType;
  }

  public void setColumnType(int columnType) {
    this.columnType = columnType;
  }

  public String getColumnTypeName() {
    return columnTypeName;
  }

  public void setColumnTypeName(String columnTypeName) {
    this.columnTypeName = columnTypeName;
  }
}