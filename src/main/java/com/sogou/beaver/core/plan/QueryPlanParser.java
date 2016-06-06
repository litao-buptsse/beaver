package com.sogou.beaver.core.plan;

import com.sogou.beaver.dao.JobDao;
import com.sogou.beaver.util.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.stream.Collectors;

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
        .map(metric -> String.format("%s(%s) AS %s",
            metric.getMethod(),
            parseMetricMethod(metric.getField()),
            metric.getAlias()))
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
        .map(filter -> String.format("%s %s %s",
            filter.getField(),
            parseFilterMethod(filter.getMethod()),
            CommonUtils.formatSQLValue(filter.getValue())))
        .collect(Collectors.joining(" AND "));
    String bucketSQL = query.getBuckets().stream()
        .map(bucket -> bucket.getField())
        .collect(Collectors.joining(", "));

    String sql = String.format("SELECT %s, %s FROM %s WHERE %s AND %s GROUP BY %s",
        bucketMetricSQL, metricSQL, tableName, timeRangeSQL, filterSQL, bucketSQL);
    LOG.debug("parsed sql: " + sql);
    return sql;
  }

  private static String parseMetricMethod(String method) {
    return method;
  }

  private static String parseFilterMethod(String method) {
    String realMethod;
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
      default:
        realMethod = null;
        break;
    }
    return realMethod;
  }
}
