package com.sogou.beaver.parser;

import java.util.List;

/**
 * Created by Tao Li on 2016/6/1.
 */
public class QueryTerm {
  private String tableName;
  private List<Metric> metrics;
  private List<Bucket> buckets;
  private List<Filter> filters;
  private TimeRange timeRange;

  class Metric {
    private String method;
    private String field;
    private String alias;
  }

  class Bucket {
    private String field;
    private String alias;
  }

  class Filter {
    private String method;
    private String field;
    private String value;
  }

  class TimeRange {
    private String startTime;
    private String endTime;
  }
}
