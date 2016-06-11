package com.sogou.beaver.resources;

import com.sogou.beaver.dao.TableInfoDao;
import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.model.TableInfo;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by Tao Li on 6/11/16.
 */
@Path("/tableInfos")
public class TableInfoResources {
  private final TableInfoDao dao;

  public TableInfoResources(TableInfoDao dao) {
    this.dao = dao;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public List<TableInfo> getAllTableInfos()
      throws ConnectionPoolException, SQLException {
    return dao.getAllTableInfos();
  }
}
