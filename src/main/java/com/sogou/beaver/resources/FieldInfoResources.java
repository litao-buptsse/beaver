package com.sogou.beaver.resources;

import com.sogou.beaver.Config;
import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.common.CommonUtils;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.sql.SQLException;

/**
 * Created by Tao Li on 6/11/16.
 */
@Path("/fieldInfos")
public class FieldInfoResources {
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Object getFieldInfos(@QueryParam("tableId") long tableId,
                              @QueryParam("callback") String callback)
      throws ConnectionPoolException, SQLException {
    return CommonUtils.formatJSONPObject(callback,
        Config.FIELD_INFO_DAO.getFieldInfosByTableId(tableId));
  }
}
