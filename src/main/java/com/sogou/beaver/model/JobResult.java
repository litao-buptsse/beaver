package com.sogou.beaver.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Created by Tao Li on 6/10/16.
 */
public class JobResult {
  private List<HeaderInfo> headerInfos;
  private List<String[]> values;

  public static class HeaderInfo {
    private int id;
    private String name;
    private String description;
    private String type;

    public HeaderInfo() {
    }

    public HeaderInfo(int id, String name, String description, String type) {
      this.id = id;
      this.name = name;
      this.description = description;
      this.type = type;
    }

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getDescription() {
      return description;
    }

    public void setDescription(String description) {
      this.description = description;
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }
  }

  public JobResult() {
  }

  public JobResult(List<HeaderInfo> headerInfos, List<String[]> values) {
    this.headerInfos = headerInfos;
    this.values = values;
  }

  @JsonProperty
  public List<HeaderInfo> getHeaderInfos() {
    return headerInfos;
  }

  public void setHeaderInfos(List<HeaderInfo> headerInfos) {
    this.headerInfos = headerInfos;
  }

  @JsonProperty
  public List<String[]> getValues() {
    return values;
  }

  public void setValues(List<String[]> values) {
    this.values = values;
  }
}
