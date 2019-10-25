package com.macys.common.pricing.services.upccsvgenerator.batch.transforms;

import com.google.common.collect.ImmutableList;
import com.macys.common.pricing.services.upccsvgenerator.batch.functions.SpannerReadOperationDoFn;
import com.macys.common.pricing.services.upccsvgenerator.batch.options.CsvGeneratorPipelineOptions;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SpannerTableReader extends PTransform<PBegin, PCollection<ReadOperation>> {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerTableReader.class);

  @Override
  public PCollection<ReadOperation> expand(PBegin input) {
    CsvGeneratorPipelineOptions csvOptions = (CsvGeneratorPipelineOptions)input.getPipeline().getOptions();
    LOG.info("Csv Options are {}", csvOptions);
    return input.getPipeline()
        .apply("Pipeline start", Create.of(ImmutableList.of("")))
        .apply("Read select columns from PROD_SKU table", ParDo.of(new SpannerReadOperationDoFn(csvOptions.getSpannerTable())));
  }
}
