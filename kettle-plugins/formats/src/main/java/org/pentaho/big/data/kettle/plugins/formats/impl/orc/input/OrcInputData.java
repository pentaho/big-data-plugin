/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package org.pentaho.big.data.kettle.plugins.formats.impl.orc.input;

import java.util.Iterator;

import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.steps.file.BaseFileInputStepData;
import org.pentaho.hadoop.shim.api.format.IPentahoInputFormat.IPentahoRecordReader;
import org.pentaho.hadoop.shim.api.format.IPentahoOrcInputFormat;

public class OrcInputData extends BaseFileInputStepData {
  IPentahoOrcInputFormat input;
  IPentahoRecordReader reader;
  Iterator<RowMetaAndData> rowIterator;
  RowMetaInterface outputRowMeta;
}
