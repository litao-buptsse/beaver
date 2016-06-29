package com.sogou.beaver.resources;

import com.sogou.beaver.Config;
import com.sogou.beaver.core.collector.FileOutputCollector;
import com.sogou.beaver.core.plan.ExecutionPlan;
import com.sogou.beaver.core.plan.ParseException;
import com.sogou.beaver.db.ConnectionPoolException;
import com.sogou.beaver.model.Job;
import com.sogou.beaver.model.JobResult;
import com.sogou.beaver.common.CommonUtils;

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
    ExecutionPlan executionPlan = ExecutionPlan.fromQueryPlan(
        job.getQueryType(), job.getQueryPlan());
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
  public Response download(@PathParam("id") long id)
      throws IOException, ConnectionPoolException, SQLException {
    Job job = Config.JOB_DAO.getJobById(id);
    return job.getHost().equals(Config.HOST) ?
        Response.ok(FileOutputCollector.getStreamingOutput(id)).
            header(HttpHeaders.CONTENT_DISPOSITION, "attachment;filename=" + id + ".csv").build() :
        CommonUtils.sendHttpRequest(
            "GET", String.format("http://%s:8080/jobs/download/%s", job.getHost(), job.getId()),
            MediaType.APPLICATION_OCTET_STREAM);
  }

  @GET
  @Path("/result/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Object getResult(@PathParam("id") long id,
                          @DefaultValue("0") @QueryParam("start") int start,
                          @DefaultValue("10") @QueryParam("length") int length,
                          @QueryParam("callback") String callback)
      throws IOException, ConnectionPoolException, SQLException {
    Job job = Config.JOB_DAO.getJobById(id);
    return CommonUtils.formatJSONPObject(callback, job.getHost().equals(Config.HOST) ?
        FileOutputCollector.getJobResult(id, start, length) :
        CommonUtils.sendHttpRequest(
            "GET", String.format("http://%s:8080/jobs/result/%s?start=%s&length=%s",
                job.getHost(), job.getId(), start, length),
            MediaType.APPLICATION_JSON).readEntity(JobResult.class));
  }
}
