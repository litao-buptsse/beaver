package com.sogou.beaver.core.plan;

/**
 * Created by Tao Li on 6/19/16.
 */
public interface Query {
  ExecutionPlan parse() throws ParseException;
}