package com.sogou.beaver.resources;

import com.sogou.beaver.core.plan.ExecutionPlan;
import com.sogou.beaver.core.plan.ParseException;
import com.sogou.beaver.core.plan.QueryPlan;
import com.sogou.beaver.core.plan.QueryPlanParser;
import com.sogou.beaver.dao.JobDao;
import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.model.Job;

import javax.ws.rs.*;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
  public void submitJob(Job job)
      throws ConnectionPoolException, SQLException, IOException, ParseException {
    QueryPlan queryTerm = QueryPlan.fromJson(job.getQueryPlan());
    ExecutionPlan executionPlan = QueryPlanParser.parse(queryTerm);
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

  @GET
  @Path("/download")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  public Response download(@QueryParam("id") long id) throws FileNotFoundException {
    return Response.ok(new FileInputStream("data/" + id + ".data"))
        .header(HttpHeaders.CONTENT_DISPOSITION, "attachment;filename=" + id + ".csv")
        .build();
  }
}
