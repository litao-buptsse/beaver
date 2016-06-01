package com.sogou.beaver.parser;

import com.sogou.beaver.execution.ExecutionEngine;
import com.sogou.beaver.execution.PrestoEngine;

/**
 * Created by Tao Li on 2016/6/1.
 */
public class QueryTermParser {
  public static ExecutionPlan parse(String queryTermString) {
    QueryTerm queryTerm = new QueryTerm(); // parse json
    return parse(queryTerm);
  }

  public static ExecutionPlan parse(QueryTerm queryTerm) {
    ExecutionEngine engine = new PrestoEngine(); // choose the right engine
    String sql = null; // parse queryTerm to sql
    ExecutionPlan executionPlan = new ExecutionPlan(engine, sql);
    return executionPlan;
  }
}
