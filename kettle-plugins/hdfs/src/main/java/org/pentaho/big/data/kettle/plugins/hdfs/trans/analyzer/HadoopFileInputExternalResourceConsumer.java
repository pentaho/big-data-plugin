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

import org.pentaho.big.data.kettle.plugins.hdfs.trans.HadoopFileInputMeta;
import org.pentaho.di.trans.steps.fileinput.text.TextFileInput;
import org.pentaho.metaverse.api.analyzer.kettle.step.BaseStepExternalResourceConsumer;

public class HadoopFileInputExternalResourceConsumer
  extends BaseStepExternalResourceConsumer<TextFileInput, HadoopFileInputMeta> {

  @Override
  public Class<HadoopFileInputMeta> getMetaClass() {
    return HadoopFileInputMeta.class;
  }

  @Override
  public boolean isDataDriven( final HadoopFileInputMeta meta ) {
    return meta.isAcceptingFilenames();
  }
}
