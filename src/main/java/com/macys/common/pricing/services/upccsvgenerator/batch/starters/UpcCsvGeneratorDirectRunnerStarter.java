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
@ConditionalOnProperty(value = "pipeline.runner.type", havingValue = "DirectRunner", matchIfMissing = true)
public class UpcCsvGeneratorDirectRunnerStarter {

  @Value("${pipeline.runner.project:fake-project}")
  private String runnerProject;

  @Value("${pipeline.config-spanner.projectId:fake-project}")
  private String spannerProject;

  @Value("${pipeline.config-spanner.instanceId}")
  private String spannerInstanceId;

  @Value("${pipeline.config-spanner.profileId:default}")
  private String spannerProfileId;

  @Value("${pipeline.config-spanner.databaseId}")
  private String spannerDatabaseId;

  @Value("${pipeline.config-spanner.prodskutable:PROD_SKU}")
  private String spannerTable;

  @Value("${pipeline.config-storage.upc-csv-storage-directory}")
  private String destinationFilePath;

  @PostConstruct
  public void runDirectRunnerPipeline() {
    log.info("Creating DirectRunner pipeline");
    String[] args = new String[]{"--project=" + runnerProject,
        "--usePublicIps=false",
        "--spannerProjectId=" + spannerProject,
        "--spannerInstanceId=" + spannerInstanceId,
        "--spannerProfileId=" + spannerProfileId,
        "--spannerDatabaseId=" + spannerDatabaseId,
        "--spannerTable=" + spannerTable,
        "--upcDestinationFilePath=" + destinationFilePath};

    DivDeptUpcCsvGeneratorJob.main(args);
  }
}
