package com.sogou.beaver;

import com.sogou.beaver.core.execution.JobExecuteController;
import com.sogou.beaver.resources.*;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

/**
 * Created by Tao Li on 5/31/16.
 */
public class BeaverApplication extends Application<Config> {
  @Override
  public void run(Config conf, Environment environment) throws Exception {
    // init static config
    Config.initStaticConfig(conf);

    // start db connection pool
    Config.POOL.start();

    // start presto connection pool
    Config.PRESTO_POOL.start();

    // start JobExecuteController
    JobExecuteController jobExecuteController = new JobExecuteController(
        Config.JOB_QUEUE_SIZE, Config.WORKER_NUM, Config.HOST);
    new Thread(jobExecuteController, "JobExecuteController").start();

    // register resources
    environment.jersey().register(new JobResources());
    environment.jersey().register(new TableInfoResources());
    environment.jersey().register(new FieldInfoResources());
    environment.jersey().register(new EnumInfoResources());
    environment.jersey().register(new MethodInfoResources());
    environment.jersey().register(new EngineInfoResources());

    // add shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        jobExecuteController.shutdown();
        Config.PRESTO_POOL.close();
        Config.POOL.close();
      }
    });
  }

  @Override
  public String getName() {
    return "beaver";
  }

  @Override
  public void initialize(Bootstrap<Config> bootstrap) {
    // nothing to do yet
  }

  public static void main(String[] args) throws Exception {
    new BeaverApplication().run(args);
  }
}
