package com.sogou.beaver.resources;

import com.sogou.beaver.dao.FieldInfoDao;
import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.model.FieldInfo;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by Tao Li on 6/11/16.
 */
@Path("/fieldInfos")
public class FieldInfoResources {
  private final FieldInfoDao dao;

  public FieldInfoResources(FieldInfoDao dao) {
    this.dao = dao;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public List<FieldInfo> getFieldInfos(@QueryParam("tableId") long tableId)
      throws ConnectionPoolException, SQLException {
    return dao.getFieldInfosByTableId(tableId);
  }
}
