package com.sogou.beaver.core.plan;

/**
 * Created by Tao Li on 2016/6/1.
 */
public class QueryTermParser {
  public static ExecutionPlan parse(QueryTerm queryTerm) {
    String engine = "presto"; // choose the right engine
    String sql = "hello, world"; // parse queryTerm to sql
    ExecutionPlan executionPlan = new ExecutionPlan(engine, sql);
    return executionPlan;
  }
}
