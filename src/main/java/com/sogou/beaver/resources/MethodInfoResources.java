package com.sogou.beaver.resources;

import com.sogou.beaver.dao.MethodInfoDao;
import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.model.MethodInfo;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.sql.SQLException;

/**
 * Created by Tao Li on 6/11/16.
 */
@Path("/methodInfos")
public class MethodInfoResources {
  private final MethodInfoDao dao;

  public MethodInfoResources(MethodInfoDao dao) {
    this.dao = dao;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public MethodInfo getMethodInfo()
      throws ConnectionPoolException, SQLException {
    return dao.getMethodInfo();
  }
}
