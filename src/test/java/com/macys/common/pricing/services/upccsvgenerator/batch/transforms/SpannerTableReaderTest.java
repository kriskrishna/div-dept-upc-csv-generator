package com.macys.common.pricing.services.upccsvgenerator.batch.transforms;

import com.macys.common.pricing.services.upccsvgenerator.batch.options.CsvGeneratorPipelineOptions;
import java.io.Serializable;
import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.ReadOperation;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Test;

public class SpannerTableReaderTest implements Serializable {

  @Test
  public void readProdSkuTable() {
    CsvGeneratorPipelineOptions options = TestPipeline.testingPipelineOptions()
        .as(CsvGeneratorPipelineOptions.class);

    Pipeline p = Pipeline.create(options);
    PCollection<ReadOperation> output = p.apply("Test Spanner Table Read Operation",
        new SpannerTableReader());

    PAssert.that(output).satisfies(new TestColumnsBeingReadFn());
    PipelineResult result = p.run();
    TestPipeline.verifyPAssertsSucceeded(p, result);

  }

  public class TestColumnsBeingReadFn implements
      SerializableFunction<Iterable<ReadOperation>, Void> {

    @Override
    public Void apply(Iterable<ReadOperation> input) {
      Assert.assertEquals(1, input.spliterator().getExactSizeIfKnown());
      input.spliterator().forEachRemaining(readOperation -> {
        Assert.assertEquals(3, readOperation.getColumns().size());
        Assert.assertTrue("Compare columns for ProdSku table query",
            readOperation.getColumns().containsAll(Arrays.asList("LocNbr", "DeptNbr", "SkuUpcNbr")));
      });
      return null;
    }
  }
}