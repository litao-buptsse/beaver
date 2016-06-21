package com.sogou.beaver.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Created by Tao Li on 6/11/16.
 */
public class MethodInfo {
  private List<MetricMethod> metricMethods;
  private List<FilterMethod> filterMethods;

  public MethodInfo() {
  }

  public MethodInfo(List<MetricMethod> metricMethods, List<FilterMethod> filterMethods) {
    this.metricMethods = metricMethods;
    this.filterMethods = filterMethods;
  }

  @JsonProperty
  public List<MetricMethod> getMetricMethods() {
    return metricMethods;
  }

  public void setMetricMethods(List<MetricMethod> metricMethods) {
    this.metricMethods = metricMethods;
  }

  @JsonProperty
  public List<FilterMethod> getFilterMethods() {
    return filterMethods;
  }

  public void setFilterMethods(List<FilterMethod> filterMethods) {
    this.filterMethods = filterMethods;
  }

  public static class MetricMethod {
    private long id;
    private String name;
    private String description;
    private String dataType;

    public MetricMethod() {
    }

    public MetricMethod(long id, String name, String description, String dataType) {
      this.id = id;
      this.name = name;
      this.description = description;
      this.dataType = dataType;
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
    public String getDataType() {
      return dataType;
    }

    public void setDataType(String dataType) {
      this.dataType = dataType;
    }
  }

  public static class FilterMethod {
    private long id;
    private String name;
    private String description;

    public FilterMethod() {
    }

    public FilterMethod(long id, String name, String description) {
      this.id = id;
      this.name = name;
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
  }
}
