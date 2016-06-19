package com.sogou.beaver.core.plan;

import com.sogou.beaver.db.JDBCConnectionPool;

/**
 * Created by Tao Li on 6/19/16.
 */
public interface Query {
  String parseEngine(JDBCConnectionPool pool);

  String parseSQL(JDBCConnectionPool pool);
}
