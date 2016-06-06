package com.sogou.beaver.resources;

import com.sogou.beaver.core.collector.FileOutputCollector;
import com.sogou.beaver.core.plan.ExecutionPlan;
import com.sogou.beaver.core.plan.ParseException;
import com.sogou.beaver.core.plan.QueryPlan;
import com.sogou.beaver.core.plan.QueryPlanParser;
import com.sogou.beaver.dao.JobDao;
import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.model.Job;
import com.sogou.beaver.util.CommonUtils;

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
                           @DefaultValue("1") @QueryParam("page") long page,
                           @DefaultValue("10") @QueryParam("size") long size)
      throws ConnectionPoolException, SQLException {
    return dao.getJobsByUserId(userId, page, size);
  }

  @GET
  @Path("/download/{id}")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  public Response download(@PathParam("id") long id) throws FileNotFoundException {
    String localFile = String.format("%s/%s.data", FileOutputCollector.getOutputRootDir(), id);
    String downloadFileName = String.format("%s.csv", id);
    return Response.ok(new FileInputStream(localFile))
        .header(HttpHeaders.CONTENT_DISPOSITION, "attachment;filename=" + downloadFileName)
        .build();
  }

  @GET
  @Path("/result/{id}")
  @Produces(MediaType.TEXT_PLAIN)
  public String getResult(@PathParam("id") long id,
                          @DefaultValue("1") @QueryParam("page") long page,
                          @DefaultValue("10") @QueryParam("size") long size)
      throws IOException {
    String localFile = String.format("%s/%s.data", FileOutputCollector.getOutputRootDir(), id);
    return CommonUtils.readFile(localFile, page, size, true);
  }
}
