package org.pentaho.big.data.kettle.plugins.formats.parquet.input;

import java.util.List;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.steps.file.BaseFileInputStepData;

public class ParquetInputData extends BaseFileInputStepData {
  ParquetInputFormat input;
  List<InputSplit> splits;
  int currentSplit;
  RecordReader reader;
  RowMetaInterface outputRowMeta;
}
