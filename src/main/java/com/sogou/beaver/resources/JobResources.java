package com.sogou.beaver.resources;

import com.sogou.beaver.dao.JobDao;
import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.db.JDBCConnectionPool;
import com.sogou.beaver.core.plan.ExecutionPlan;
import com.sogou.beaver.core.plan.QueryTerm;
import com.sogou.beaver.core.plan.QueryTermParser;
import com.sogou.beaver.model.Job;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by Tao Li on 6/1/16.
 */
@Path("/jobs")
public class JobResources {
  private final JobDao dao;

  public JobResources(JobDao dao) {
    this.dao = dao;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public void submitTask(Job job) throws ConnectionPoolException, SQLException, IOException {
    QueryTerm queryTerm = QueryTerm.fromJson(job.getQueryTerm());
    ExecutionPlan executionPlan = QueryTermParser.parse(queryTerm);
    job.setExecutionPlan(executionPlan.toJson());
    dao.createJob(job);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public List<Job> getJobs(@QueryParam("userId") String userId,
                           @QueryParam("page") long page, @QueryParam("size") long size)
      throws ConnectionPoolException, SQLException {
    return dao.getJobsByUserId(userId, page, size);
  }
}
