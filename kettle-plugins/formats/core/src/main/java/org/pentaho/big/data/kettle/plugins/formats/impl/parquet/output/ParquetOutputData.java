/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/


package org.pentaho.big.data.kettle.plugins.formats.impl.parquet.output;

import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.hadoop.shim.api.format.IPentahoOutputFormat.IPentahoRecordWriter;
import org.pentaho.hadoop.shim.api.format.IPentahoParquetOutputFormat;

public class ParquetOutputData extends BaseStepData implements StepDataInterface {

  public IPentahoParquetOutputFormat output;
  public IPentahoRecordWriter writer;
}
