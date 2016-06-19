package com.sogou.beaver.core.plan;

import com.sogou.beaver.dao.TableInfoDao;
import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.db.JDBCConnectionPool;
import com.sogou.beaver.model.TableInfo;
import com.sogou.beaver.util.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by Tao Li on 2016/6/1.
 */
public class QueryPlanParser {
  private final static Logger LOG = LoggerFactory.getLogger(QueryPlanParser.class);

  private static JDBCConnectionPool pool;

  public static void setJDBCConnectionPool(JDBCConnectionPool pool) {
    QueryPlanParser.pool = pool;
  }

  public static ExecutionPlan parse(QueryPlan queryPlan) throws ParseException {
    String engine = parseEngine(queryPlan);
    String sql = parseSQL(queryPlan);
    ExecutionPlan executionPlan = new ExecutionPlan(engine, sql);
    return executionPlan;
  }

  private static long getTimeIntervalMinutes(String startTime, String endTime, String frequency)
      throws ParseException {
    String timeFormat;
    switch (frequency) {
      case "DAY":
        timeFormat = "yyyyMMdd";
        break;
      case "HOUR":
        timeFormat = "yyyyMMddHH";
        break;
      case "5MIN":
        timeFormat = "yyyyMMddHHmm";
        break;
      default:
        throw new ParseException("Not support table frequency: " + frequency);
    }

    long startTimestamp = CommonUtils.convertStringToTimestamp(startTime, timeFormat);
    long endTimestamp = CommonUtils.convertStringToTimestamp(endTime, timeFormat);
    return (endTimestamp - startTimestamp) / 1000 / 60 + 60;
  }

  private static String parseEngine(QueryPlan queryPlan) throws ParseException {
    switch (queryPlan.getType()) {
      case "raw":
        // TODO support analysis raw sql complexity and choose the right engine
        return "spark-sql";
      case "compound":
        try {
          QueryPlan.CompoundQuery query = QueryPlan.CompoundQuery.fromJson(queryPlan.getQuery());
          try {
            TableInfoDao dao = new TableInfoDao(pool);
            TableInfo tableInfo = dao.getTableInfoByName(query.getTableName());
            if (tableInfo != null) {
              long timeIntervalMinutes = getTimeIntervalMinutes(
                  query.getTimeRange().getStartTime(),
                  query.getTimeRange().getEndTime(),
                  tableInfo.getFrequency());
              if (query.getTableName().startsWith("custom.") && timeIntervalMinutes <= 1440) {
                return "presto";
              }
            }
          } catch (ConnectionPoolException | SQLException e) {
            // ignore
          }
          return "spark-sql";
        } catch (IOException e) {
          throw new ParseException("Failde to parse query: " + queryPlan.getQuery());
        }
      default:
        throw new ParseException("Not support query type: " + queryPlan.getType());
    }
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
