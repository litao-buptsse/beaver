package com.sogou.beaver.resources;

import com.sogou.beaver.dao.EnumInfoDao;
import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.util.CommonUtils;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.sql.SQLException;

/**
 * Created by Tao Li on 6/11/16.
 */
@Path("/enumInfos")
public class EnumInfoResources {
  private final EnumInfoDao dao;

  public EnumInfoResources(EnumInfoDao dao) {
    this.dao = dao;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Object getEnumInfos(@QueryParam("fieldId") long fieldId,
                             @QueryParam("callback") String callback)
      throws ConnectionPoolException, SQLException {
    return CommonUtils.formatJSONPObject(callback, dao.getEnumInfosByFieldId(fieldId));
  }
}
