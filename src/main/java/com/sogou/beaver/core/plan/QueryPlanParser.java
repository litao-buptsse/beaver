package com.sogou.beaver.core.plan;

import com.sogou.beaver.util.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by Tao Li on 2016/6/1.
 */
public class QueryPlanParser {
  private final static Logger LOG = LoggerFactory.getLogger(QueryPlanParser.class);

  public static ExecutionPlan parse(QueryPlan queryPlan) throws ParseException {
    String engine = parseEngine(queryPlan);
    String sql = parseSQL(queryPlan);
    ExecutionPlan executionPlan = new ExecutionPlan(engine, sql);
    return executionPlan;
  }

  private static String parseEngine(QueryPlan queryPlan) {
    return "presto";
  }

  private static String parseSQL(QueryPlan queryPlan) throws ParseException {
    switch (queryPlan.getType()) {
      case "raw":
        return queryPlan.getQuery();
      case "compound":
        try {
          QueryPlan.CompoundQuery query = QueryPlan.CompoundQuery.fromJson(queryPlan.getQuery());
          return parseCompoundQuery(query);
        } catch (IOException e) {
          throw new ParseException("Failde to parse query: " + queryPlan.getQuery());
        }
      default:
        throw new ParseException("Not support query type: " + queryPlan.getType());
    }
  }

  // TODO support more complex compound query
  private static String parseCompoundQuery(QueryPlan.CompoundQuery query) {
    String tableName = query.getTableName();
    String metricSQL = query.getMetrics().stream()
        .map(metric -> String.format("%s AS %s",
            parseMetric(metric.getMethod(), metric.getField()), metric.getAlias()))
        .collect(Collectors.joining(", "));
    String bucketMetricSQL = query.getBuckets().stream()
        .map(bucket -> String.format("%s AS %s",
            bucket.getField(),
            bucket.getAlias()))
        .collect(Collectors.joining(", "));
    String timeRangeSQL = String.format("logdate>=%s AND logdate<=%s",
        CommonUtils.formatSQLValue(query.getTimeRange().getStartTime()),
        CommonUtils.formatSQLValue(query.getTimeRange().getEndTime()));
    String filterSQL = query.getFilters().stream()
        .map(filter -> parseFilter(
            filter.getMethod(), filter.getField(), filter.getValue()))
        .collect(Collectors.joining(" AND "));
    String bucketSQL = query.getBuckets().stream()
        .map(bucket -> bucket.getField())
        .collect(Collectors.joining(", "));

    String sql = String.format("SELECT %s, %s FROM %s WHERE %s AND %s GROUP BY %s",
        bucketMetricSQL, metricSQL, tableName, timeRangeSQL, filterSQL, bucketSQL);
    LOG.debug("parsed sql: " + sql);
    return sql;
  }

  private static String parseMetric(String method, String field) {
    switch (method) {
      case "count_distinct":
        return String.format("count(DISTINCT %s)", field);
      default:
        return String.format("%s(%s)", method, field);
    }
  }

  private static String parseFilter(String method, String field, String value) {
    String realMethod = method;
    String realValue = CommonUtils.formatSQLValue(value);
    switch (method) {
      case "eq":
        realMethod = "=";
        break;
      case "ne":
        realMethod = "!=";
        break;
      case "gt":
        realMethod = ">";
        break;
      case "ge":
        realMethod = ">=";
        break;
      case "lt":
        realMethod = "<";
        break;
      case "le":
        realMethod = "<=";
        break;
      case "in":
        realMethod = "IN";
        realValue = String.format("(%s)", Stream.of(value.split(","))
            .map(v -> CommonUtils.formatSQLValue(v.trim())).collect(Collectors.joining(", ")));
        break;
      case "not_in":
        realMethod = "NOT IN";
        realValue = String.format("(%s)", Stream.of(value.split(","))
            .map(v -> CommonUtils.formatSQLValue(v.trim())).collect(Collectors.joining(", ")));
        break;
    }
    return String.format("%s %s %s", field, realMethod, realValue);
  }
}
