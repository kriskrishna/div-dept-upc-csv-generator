package com.macys.common.pricing.services.upccsvgenerator.batch.starters;

import com.macys.common.pricing.services.upccsvgenerator.batch.job.DivDeptUpcCsvGeneratorJob;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@Profile("!kubernetes")
@ConditionalOnProperty(value = "pipeline.runner.type", havingValue = "DataflowRunner", matchIfMissing = false)
public class UpcCsvGeneratorDataFlowRunnerStarter {

  @Value("${pipeline.runner.project}")
  private String project;

  @Value("${pipeline.runner.staging-bucket}")
  private String stagingLocation;

  @Value("${pipeline.runner.temp-bucket}")
  private String tempLocation;

  @Value("${pipeline.runner.subnetwork}")
  private String subnetwork;

  @Value("${pipeline.runner.template-path}")
  private String templateLocation;

  @PostConstruct
  public void runDataflowPipeline() {
    log.info("Creating DataflowRunner pipeline");
    String[] args = new String[]{"--project=" + project, "--stagingLocation=" + stagingLocation,
        "--subnetwork=" + subnetwork, "--usePublicIps=false",
        "--streaming=false",
        "--appName=upccsvgeneratorjob",
        "--templateLocation=" + templateLocation, "--tempLocation=" + tempLocation,
        "--runner=DataflowRunner",
        "--workerLogLevelOverrides={\"com.macys.common.pricing.services.upccsvgenerator.batch\":\"DEBUG\",\"com.google.cloud.dataflow\":\"INFO\"}"};
    DivDeptUpcCsvGeneratorJob.main(args);
  }
}
