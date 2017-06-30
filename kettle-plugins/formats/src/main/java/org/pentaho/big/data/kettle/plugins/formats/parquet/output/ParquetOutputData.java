package org.pentaho.big.data.kettle.plugins.formats.parquet.output;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetWriter;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class ParquetOutputData extends BaseStepData implements StepDataInterface {

  public RowMetaInterface outputRowMeta;
  
  public Schema avroSchema;
  
  public ParquetWriter<GenericRecord> parquetWriter;
  
}
