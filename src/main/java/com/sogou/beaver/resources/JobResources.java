package com.sogou.beaver.resources;

import com.sogou.beaver.Config;
import com.sogou.beaver.core.collector.FileOutputCollector;
import com.sogou.beaver.core.plan.ExecutionPlan;
import com.sogou.beaver.core.plan.ParseException;
import com.sogou.beaver.core.plan.QueryPlanParser;
import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.model.Job;
import com.sogou.beaver.util.CommonUtils;

import javax.ws.rs.*;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.sql.SQLException;

/**
 * Created by Tao Li on 6/1/16.
 */
@Path("/jobs")
public class JobResources {
  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public void submitJob(Job job)
      throws ConnectionPoolException, SQLException, IOException, ParseException {
    ExecutionPlan executionPlan = QueryPlanParser.parse(job.getQueryType(), job.getQueryPlan());
    job.setExecutionPlan(CommonUtils.toJson(executionPlan));
    Config.JOB_DAO.createJob(job);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Object getJobs(@QueryParam("userId") String userId,
                        @DefaultValue("0") @QueryParam("start") int start,
                        @DefaultValue("10") @QueryParam("length") int length,
                        @QueryParam("callback") String callback)
      throws ConnectionPoolException, SQLException {
    return CommonUtils.formatJSONPObject(callback,
        Config.JOB_DAO.getJobsByUserId(userId, start, length));
  }

  @GET
  @Path("/download/{id}")
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  public Response download(@PathParam("id") long id) throws IOException {
    String downloadFile = String.format("%s.csv", id);
    return Response.ok(FileOutputCollector.getStreamingOutput(id))
        .header(HttpHeaders.CONTENT_DISPOSITION, "attachment;filename=" + downloadFile)
        .build();
  }

  @GET
  @Path("/result/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Object getResult(@PathParam("id") long id,
                          @DefaultValue("0") @QueryParam("start") int start,
                          @DefaultValue("10") @QueryParam("length") int length,
                          @QueryParam("callback") String callback) throws IOException {
    return CommonUtils.formatJSONPObject(callback,
        FileOutputCollector.getJobResult(id, start, length));
  }
}
