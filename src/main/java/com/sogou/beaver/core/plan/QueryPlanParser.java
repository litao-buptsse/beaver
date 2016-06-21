package com.sogou.beaver.core.plan;

import com.sogou.beaver.Config;
import com.sogou.beaver.util.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by Tao Li on 2016/6/1.
 */
public class QueryPlanParser {
  public static ExecutionPlan parse(String queryType, String queryPlan) throws ParseException {
    Query query;
    try {
      switch (queryType) {
        case Config.QUERY_TYPE_RAW:
          query = CommonUtils.fromJson(queryPlan, RawQuery.class);
          break;
        case Config.QUERY_TYPE_COMPOUND:
          query = CommonUtils.fromJson(queryPlan, CompoundQuery.class);
          break;
        default:
          throw new ParseException("Not support query type: " + queryType);
      }
    } catch (IOException e) {
      throw new ParseException("Fail to parse query plan: " + queryType);
    }

    return new ExecutionPlan(query.parseEngine(), query.parseSQL(), query.parseInfo());
  }
}
