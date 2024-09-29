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


package org.pentaho.big.data.kettle.plugins.hdfs.trans.analyzer;

import org.pentaho.big.data.kettle.plugins.hdfs.trans.HadoopFileOutputMeta;
import org.pentaho.di.trans.steps.fileinput.text.TextFileInput;
import org.pentaho.metaverse.api.analyzer.kettle.step.BaseStepExternalResourceConsumer;

public class HadoopFileOutputExternalResourceConsumer
  extends BaseStepExternalResourceConsumer<TextFileInput, HadoopFileOutputMeta> {

  @Override
  public Class<HadoopFileOutputMeta> getMetaClass() {
    return HadoopFileOutputMeta.class;
  }

  @Override
  public boolean isDataDriven( final HadoopFileOutputMeta meta ) {
    return meta.isFileNameInField();
  }
}
