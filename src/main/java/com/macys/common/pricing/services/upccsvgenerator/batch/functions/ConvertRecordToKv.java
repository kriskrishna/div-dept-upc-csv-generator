package com.macys.common.pricing.services.upccsvgenerator.batch.functions;

import com.google.cloud.spanner.Struct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConvertRecordToKv extends DoFn<Struct, KV<String, String>> {

  private static final Logger LOG = LoggerFactory.getLogger(ConvertRecordToKv.class);

  @ProcessElement
  public void process(ProcessContext context) {
    Struct elem = context.element();
    LOG.debug("Record being read is {} ", elem);
    Long divNbr = elem.getLong(0);
    Long deptNbr = elem.getLong(1);
    Long skuUpcNbr = elem.getLong(2);
    context.output(KV.of(String.format("%d_%d", divNbr, deptNbr), String.format("%d", skuUpcNbr)));
  }
}
