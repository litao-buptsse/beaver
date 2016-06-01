package com.sogou.beaver.parser;

import com.sogou.beaver.execution.ExecutionEngine;

/**
 * Created by Tao Li on 2016/6/1.
 */
public class ExecutionPlan {
  private ExecutionEngine engine;
  private String sql;

  public ExecutionPlan(ExecutionEngine engine, String sql) {
    this.engine = engine;
    this.sql = sql;
  }

  public void execute() {
    engine.execute(sql);
  }
}
