package org.pentaho.big.data.kettle.plugins.formats.parquet.input;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public class PentahoParquetReadSupport extends ReadSupport<Group> {
  public static MessageType schema; //TODO

  public PentahoParquetReadSupport() {
  }

  @Override
  public ReadContext init( Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema ) {
    this.schema = fileSchema;
    return new ReadContext( fileSchema, new HashMap<String, String>() );
  }

  @Override
  public RecordMaterializer<Group> prepareForRead( Configuration configuration, Map<String, String> keyValueMetaData,
      MessageType fileSchema, org.apache.parquet.hadoop.api.ReadSupport.ReadContext readContext ) {
    return new GroupRecordConverter( schema );
  }
}
