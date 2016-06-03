package com.sogou.beaver;

import com.sogou.beaver.core.engine.FileOutputCollector;
import com.sogou.beaver.core.execution.JobExecuteController;
import com.sogou.beaver.dao.JobDao;
import com.sogou.beaver.db.JDBCConnectionPool;
import com.sogou.beaver.resources.JobResources;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import java.io.File;
import java.util.Properties;

/**
 * Created by Tao Li on 5/31/16.
 */
public class BeaverApplication extends Application<BeaverConfiguration> {
  @Override
  public void run(BeaverConfiguration configuration, Environment environment) throws Exception {
    // init FileOutputCollector output root dir
    FileOutputCollector.initOutputRootDir(configuration.getOutputCollectorRootDir());

    // start mysql connection pool
    JDBCConnectionPool mysqlConnectionPool = configuration.constructJDBCConnectionPool(
        configuration.getMysqlConfiguration());
    mysqlConnectionPool.start();
    JobDao jobDao = new JobDao(mysqlConnectionPool);

    // start presto connection pool
    Properties info = new Properties();
    info.setProperty("user", "presto");
    JDBCConnectionPool prestoConnectionPool = configuration.constructJDBCConnectionPool(
        configuration.getPrestoConfiguration(), info);
    prestoConnectionPool.start();

    // start JobExecuteController
    JobExecuteController jobExecuteController = new JobExecuteController(
        jobDao, prestoConnectionPool);
    new Thread(jobExecuteController, "JobExecuteController").start();

    // register resources
    environment.jersey().register(new JobResources(jobDao));

    // add shutdown hook
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
