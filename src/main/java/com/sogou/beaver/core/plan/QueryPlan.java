package com.sogou.beaver.core.plan;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;

/**
 * Created by Tao Li on 2016/6/1.
 */
public class QueryPlan {
  private String type;
  private String query;

  static class CompoundQuery {
    private String tableName;
    private List<Metric> metrics;
    private List<Bucket> buckets;
    private List<Filter> filters;
    private TimeRange timeRange;

    static class Metric {
      private String method;
      private String field;
      private String alias;

      public Metric() {
      }

      public Metric(String method, String field, String alias) {
        this.method = method;
        this.field = field;
        this.alias = alias;
      }

      public String getAlias() {
        return alias != null && !alias.equals("") ? alias : String.format("%s_%s", method, field);
      }

      public void setAlias(String alias) {
        this.alias = alias;
      }

      public String getField() {
        return field;
      }

      public void setField(String field) {
        this.field = field;
      }

      public String getMethod() {
        return method;
      }

      public void setMethod(String method) {
        this.method = method;
      }
    }

    static class Bucket {
      private String field;
      private String alias;

      public Bucket() {
      }

      public Bucket(String field, String alias) {
        this.field = field;
        this.alias = alias;
      }

      public String getField() {
        return field;
      }

      public void setField(String field) {
        this.field = field;
      }

      public String getAlias() {
        return alias != null && !alias.equals("") ? alias : field;
      }

      public void setAlias(String alias) {
        this.alias = alias;
      }
    }

    static class Filter {
      private String method;
      private String field;
      private String value;

      public Filter() {
      }

      public Filter(String method, String field, String value) {
        this.method = method;
        this.field = field;
        this.value = value;
      }

      public String getMethod() {
        return method;
      }

      public void setMethod(String method) {
        this.method = method;
      }

      public String getField() {
        return field;
      }

      public void setField(String field) {
        this.field = field;
      }

      public String getValue() {
        return value;
      }

      public void setValue(String value) {
        this.value = value;
      }
    }

    static class TimeRange {
      private String startTime;
      private String endTime;

      public TimeRange() {
      }

      public TimeRange(String startTime, String endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
      }

      public String getStartTime() {
        return startTime;
      }

      public void setStartTime(String startTime) {
        this.startTime = startTime;
      }

      public String getEndTime() {
        return endTime;
      }

      public void setEndTime(String endTime) {
        this.endTime = endTime;
      }
    }

    public CompoundQuery() {
    }

    public CompoundQuery(String tableName,
                         List<Metric> metrics, List<Bucket> buckets, List<Filter> filters,
                         TimeRange timeRange) {
      this.tableName = tableName;
      this.metrics = metrics;
      this.buckets = buckets;
      this.filters = filters;
      this.timeRange = timeRange;
    }

    public String getTableName() {
      return tableName;
    }

    public void setTableName(String tableName) {
      this.tableName = tableName;
    }

    public List<Metric> getMetrics() {
      return metrics;
    }

    public void setMetrics(List<Metric> metrics) {
      this.metrics = metrics;
    }

    public List<Bucket> getBuckets() {
      return buckets;
    }

    public void setBuckets(List<Bucket> buckets) {
      this.buckets = buckets;
    }

    public List<Filter> getFilters() {
      return filters;
    }

    public void setFilters(List<Filter> filters) {
      this.filters = filters;
    }

    public TimeRange getTimeRange() {
      return timeRange;
    }

    public void setTimeRange(TimeRange timeRange) {
      this.timeRange = timeRange;
    }

    public static CompoundQuery fromJson(String json) throws IOException {
      return new ObjectMapper().readValue(json.getBytes(), CompoundQuery.class);
    }

    public String toJson() throws JsonProcessingException {
      return new ObjectMapper().writeValueAsString(this);
    }
  }

  public QueryPlan() {
  }

  public QueryPlan(String type, String query) {
    this.type = type;
    this.query = query;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public static QueryPlan fromJson(String json) throws IOException {
    return new ObjectMapper().readValue(json.getBytes(), QueryPlan.class);
  }

  public String toJson() throws JsonProcessingException {
    return new ObjectMapper().writeValueAsString(this);
  }
}
