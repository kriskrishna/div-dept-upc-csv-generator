package com.macys.common.pricing.services.upccsvgenerator.batch.functions;

import com.google.cloud.spanner.Struct;
import com.macys.common.pricing.services.upccsvgenerator.batch.options.CsvGeneratorPipelineOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Test;

public class ConvertRecordToKvTest{

  @Test
  public void testConvertRecordsToKv(){
    CsvGeneratorPipelineOptions options = TestPipeline.testingPipelineOptions()
        .as(CsvGeneratorPipelineOptions.class);
    Pipeline p = Pipeline.create(options);

    PCollection<KV<String, String>> output = p
        .apply(Create.of(getMockedSpannerStructs()))
        .apply(ParDo.of(new ConvertRecordToKv()));

    PAssert.that(output).satisfies(new PCIAssertFunction());
    PipelineResult result = p.run();
    TestPipeline.verifyPAssertsSucceeded(p, result);
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
  private static class PCIAssertFunction implements
      SerializableFunction<Iterable<KV<String, String>>, Void> {

    @Override
    public Void apply(Iterable<KV<String, String>> input) {
      Assert.assertEquals(4, input.spliterator().getExactSizeIfKnown());
      AtomicInteger counter = new AtomicInteger(0);
      input.spliterator().forEachRemaining(kv -> {
        if ("12_102".equals(kv.getKey())) {
          Assert.assertEquals("987654003", kv.getValue());
        } else if ("13_201".equals(kv.getKey())) {
          Assert.assertEquals("987654004", kv.getValue());
        } else if("12_101".equals(kv.getKey())){
          counter.incrementAndGet();
        }
      });
      Assert.assertEquals(2, counter.get());
      return null;
    }
  }
}