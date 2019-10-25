package com.macys.common.pricing.services.upccsvgenerator.batch.functions;

import com.google.cloud.spanner.Struct;
import com.macys.common.pricing.services.upccsvgenerator.batch.job.DivDeptUpcCsvGeneratorJob;
import com.macys.common.pricing.services.upccsvgenerator.batch.options.CsvGeneratorPipelineOptions;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DynamicFileWriterTest {

  @Rule
  public transient TemporaryFolder tmpFolder = new TemporaryFolder();
  Path firstPath;

  @Test
  public void testConvertRecordsToKv() throws IOException {
    CsvGeneratorPipelineOptions options = TestPipeline.testingPipelineOptions()
        .as(CsvGeneratorPipelineOptions.class);
    firstPath = tmpFolder.newFolder("output").toPath();
    options.setUpcDestinationFilePath(StaticValueProvider.of(firstPath.toString()));
    Pipeline p = Pipeline.create(options);

    WriteFilesResult<String> output = p.apply(Create.of(getMockedSpannerStructs()))
        .apply(ParDo.of(new ConvertRecordToKv()))
        .apply("Group", GroupByKey.<String, String>create())
        .apply("Dynamic Write", DivDeptUpcCsvGeneratorJob.dynamicFileWriter(options));
    PipelineResult result = p.run();
    assertFilesCreated();
  }

  private void assertFilesCreated() throws IOException{
    List<String> files = Files.walk(Paths.get(firstPath.toString())).filter(Files::isRegularFile)
        .map(f -> f.getFileName().toString()).collect(Collectors.toList());
    Assert.assertEquals(3, files.size());
    Arrays.asList(files.containsAll(Arrays.asList("12_101.csv", "12_102.csv", "13_201.csv")));

  }

  @After
  public void cleanUp(){
    //Remove the temporary directory
    File outputDir = new File(firstPath.toString());
    outputDir.delete();
  }

  private static List<Struct> getMockedSpannerStructs() {
    List<Struct> structs = new ArrayList<>();
    Struct struct = Struct.newBuilder().set("LocNbr").to(12)
        .set("DeptNbr").to(101)
        .set("SkuUpcNbr").to(987654001)
        .build();
    structs.add(struct);struct = Struct.newBuilder().set("LocNbr").to(12)
        .set("DeptNbr").to(101)
        .set("SkuUpcNbr").to(987654002)
        .build();
    structs.add(struct);struct = Struct.newBuilder().set("LocNbr").to(12)
        .set("DeptNbr").to(102)
        .set("SkuUpcNbr").to(987654003)
        .build();
    structs.add(struct);struct = Struct.newBuilder().set("LocNbr").to(13)
        .set("DeptNbr").to(201)
        .set("SkuUpcNbr").to(987654004)
        .build();
    structs.add(struct);
    return structs;
  }
}