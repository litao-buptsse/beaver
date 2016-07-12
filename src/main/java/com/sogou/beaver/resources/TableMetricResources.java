package com.sogou.beaver.resources;

import com.sogou.beaver.Config;
import com.sogou.beaver.common.CommonUtils;
import com.sogou.beaver.db.ConnectionPoolException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.sql.SQLException;

/**
 * Created by Tao Li on 6/11/16.
 */
@Path("/tableMetrics")
public class TableMetricResources {
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Object getTableMetrics(@QueryParam("tableId") long viewId,
                                @QueryParam("callback") String callback)
      throws ConnectionPoolException, SQLException {
    return CommonUtils.formatJSONPObject(callback,
        Config.TABLE_METRIC_DAO.getTableMetricsByViewId(viewId));
  }
}
