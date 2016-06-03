package com.sogou.beaver.core.plan;

import java.io.IOException;

/**
 * Created by Tao Li on 2016/6/1.
 */
public class QueryPlanParser {
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
          return "hello, world";
        } catch (IOException e) {
          throw new ParseException("Failde to parse query: " + queryPlan.getQuery());
        }
      default:
        throw new ParseException("Not support query type: " + queryPlan.getType());
    }
  }
}
