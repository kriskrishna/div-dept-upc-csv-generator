package com.macys.common.pricing.services.upccsvgenerator.batch.job;

import com.macys.common.pricing.services.upccsvgenerator.batch.functions.ConvertRecordToKv;
import com.macys.common.pricing.services.upccsvgenerator.batch.functions.CustomFileNaming;
import com.macys.common.pricing.services.upccsvgenerator.batch.options.CsvGeneratorPipelineOptions;
import com.macys.common.pricing.services.upccsvgenerator.batch.transforms.SpannerTableReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.Write;
import org.apache.beam.sdk.io.FileIO.Write.FileNaming;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;

@Slf4j
public class DivDeptUpcCsvGeneratorJob {

  public static void main(String... args) {
    log.info("DivDeptUpcCsvGeneratorJob Entry");
    try {
      PipelineOptionsFactory.register(CsvGeneratorPipelineOptions.class);
      CsvGeneratorPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
          .withValidation()
          .as(CsvGeneratorPipelineOptions.class);
      Pipeline pipeline = Pipeline.create(options);

      SpannerConfig spannerConfig =
          SpannerConfig.create()
              .withProjectId(options.getSpannerProjectId())
              .withInstanceId(options.getSpannerInstanceId())
              .withDatabaseId(options.getSpannerDatabaseId());


      pipeline
          .apply("Create Read Operation Query", new SpannerTableReader())
          .apply("Read all records in parallel", SpannerIO.readAll().withSpannerConfig(spannerConfig))
          .apply("Convert to Div-Dept Key and value as KV<DivDept, Upc#>",
              ParDo.of(new ConvertRecordToKv()))
          .apply("Group all UPCs under DivDept", GroupByKey.<String, String>create())
          .apply("Write records to Div_Dept.CSV file", dynamicFileWriter(options));

      pipeline.run();
    } catch (Exception e) {
      log.error("DivDeptUpcCsvGeneratorJob has failed", e);
    }
  }

  public static Write<String, KV<String, Iterable<String>>> dynamicFileWriter(CsvGeneratorPipelineOptions options) {
    return FileIO.<String, KV<String, Iterable<String>>>writeDynamic()
        .by(KV::getKey)
        .withDestinationCoder(StringUtf8Coder.of())
        .via(Contextful.fn(DivDeptUpcCsvGeneratorJob::getContentString), TextIO.sink())
        .to(options.getUpcDestinationFilePath())
        .withNaming(type -> customNaming(type, ".csv"));
  }

  private static FileNaming customNaming(String prefix, String suffix) {
    return CustomFileNaming.builder().fileNameStr(prefix+suffix).build();
  }

  private static String getContentString(KV<String, Iterable<String>> divDeptCol) {
    StringBuilder upcList = new StringBuilder("");
    divDeptCol.getValue().forEach(upc -> upcList.append(",").append(upc));
    String[] divDepTokens = divDeptCol.getKey().split("_");
    return divDepTokens[0]+","+divDepTokens[1]+upcList.toString();
  }
}
