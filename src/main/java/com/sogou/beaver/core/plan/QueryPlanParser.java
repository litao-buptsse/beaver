package com.sogou.beaver.core.plan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by Tao Li on 2016/6/1.
 */
public class QueryPlanParser {
  private final static Logger LOG = LoggerFactory.getLogger(QueryPlanParser.class);

  public static ExecutionPlan parse(QueryPlan queryPlan) throws ParseException {
    Query query;
    try {
      switch (queryPlan.getType()) {
        case "raw":
          query = RawQuery.fromJson(queryPlan.getQuery());
          break;
        case "compound":
          query = CompoundQuery.fromJson(queryPlan.getQuery());
          break;
        default:
          throw new ParseException("Not support query type: " + queryPlan.getType());
      }
    } catch (IOException e) {
      throw new ParseException("Fail to parse query plan: " + queryPlan.getQuery());
    }

    return new ExecutionPlan(query.parseEngine(), query.parseSQL(), query.parseInfo());
  }
}
