package com.macys.common.pricing.services.upccsvgenerator.batch.functions;

import java.io.File;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import lombok.Builder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO.Write.FileNaming;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;

@Builder
public class CustomFileNaming implements FileNaming, Serializable {

  String fileNameStr;

  @Override
  public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex, Compression compression) {
    return getDatePrefix().concat(fileNameStr);
  }

  private String getDatePrefix() {
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");
    return LocalDate.now(ZoneId.of("America/New_York")).format(dateTimeFormatter)
        .concat(File.separator);
  }

}
