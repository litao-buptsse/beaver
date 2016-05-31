package com.sogou.beaver;

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
    environment.jersey().register(new JobResources());
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
