package com.macys.common.pricing.services.upccsvgenerator.batch.options;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface CsvGeneratorPipelineOptions extends PipelineOptions {

  ValueProvider<String> getUpcDestinationFilePath();

  void setUpcDestinationFilePath(ValueProvider<String> value);

  ValueProvider<String> getSpannerProjectId();

  void setSpannerProjectId(ValueProvider<String> value);

  ValueProvider<String> getSpannerInstanceId();

  void setSpannerInstanceId(ValueProvider<String> value);

  ValueProvider<String> getSpannerProfileId();

  void setSpannerProfileId(ValueProvider<String> value);

  ValueProvider<String> getSpannerDatabaseId();

  void setSpannerDatabaseId(ValueProvider<String> value);

  ValueProvider<String> getSpannerTable();

  void setSpannerTable(ValueProvider<String> value);

}
