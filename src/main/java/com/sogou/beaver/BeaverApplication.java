package com.sogou.beaver;

import com.sogou.beaver.core.execution.JobExecuteController;
import com.sogou.beaver.dao.JobDao;
import com.sogou.beaver.db.JDBCConnectionPool;
import com.sogou.beaver.resources.JobResources;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

/**
 * Created by Tao Li on 5/31/16.
 */
public class BeaverApplication extends Application<BeaverConfiguration> {
  @Override
  public void run(BeaverConfiguration configuration, Environment environment) throws Exception {
    JDBCConnectionPool mysqlConnectionPool = configuration.constructMysqlConnectionPool(
        configuration.getMysqlConfiguration());
    mysqlConnectionPool.start();

    JobDao jobDao = new JobDao(mysqlConnectionPool);

    JobExecuteController jobExecuteController = new JobExecuteController(jobDao);
    new Thread(jobExecuteController, "JobExecuteController").start();

    environment.jersey().register(new JobResources(jobDao));

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        jobExecuteController.shutdown();
      }
    });
  }

  @Override
  public String getName() {
    return "beaver";
  }

  @Override
  public void initialize(Bootstrap<BeaverConfiguration> bootstrap) {
    // nothing to do yet
  }

  public static void main(String[] args) throws Exception {
    new BeaverApplication().run(args);
  }
}
