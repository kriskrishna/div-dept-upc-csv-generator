package com.macys.common.pricing.services.upccsvgenerator.batch.functions;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;

public class SpannerReadOperationDoFn extends DoFn<String, ReadOperation> {

  ValueProvider<String> spannerTableName;

  public SpannerReadOperationDoFn(ValueProvider<String> spannerTableName){
    this.spannerTableName = spannerTableName;
  }

  private static final String PROD_SKU_DIV_COLUMN = "LocNbr";
  private static final String PROD_SKU_DEPT_COLUMN = "DeptNbr";
  private static final String PROD_SKU_UPC_COLUMN = "SkuUpcNbr";

  @ProcessElement
  public void process(ProcessContext context) {
    context.output(getReadOperation(spannerTableName.get()));
  }

  private ReadOperation getReadOperation(String spannerTable) {
    return ReadOperation.create()
        .withColumns(getProdSkuColumns())
        .withTable(spannerTable);
  }

  private List<String> getProdSkuColumns() {
    return Arrays.asList(PROD_SKU_DIV_COLUMN, PROD_SKU_DEPT_COLUMN, PROD_SKU_UPC_COLUMN);
  }
}
