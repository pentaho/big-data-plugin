/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.big.data.kettle.plugins.formats.impl.parquet.output;

import org.pentaho.big.data.api.cluster.service.locator.impl.NamedClusterServiceLocatorImpl;
import org.pentaho.big.data.impl.cluster.NamedClusterManager;
import org.pentaho.big.data.kettle.plugins.formats.impl.NamedClusterResolver;
import org.pentaho.big.data.kettle.plugins.formats.parquet.output.ParquetOutputMetaBase;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;

@Step( id = "ParquetOutput", image = "PO.svg", name = "ParquetOutput.Name", description = "ParquetOutput.Description",
  categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.BigData",
  documentationUrl = "mk-95pdia003/pdi-transformation-steps/parquet-output",
  i18nPackageName = "org.pentaho.di.trans.steps.parquet" )
@InjectionSupported( localizationPrefix = "ParquetOutput.Injection.", groups = { "FILENAME_LINES", "FIELDS" }, hide = {
  "FIELD_POSITION", "FIELD_LENGTH", "FIELD_IGNORE", "FIELD_FORMAT", "FIELD_PRECISION", "FIELD_CURRENCY",
  "FIELD_DECIMAL", "FIELD_GROUP", "FIELD_REPEAT", "FIELD_TRIM_TYPE", "FIELD_NULL_STRING"
} )
public class ParquetOutputMeta extends ParquetOutputMetaBase {

  private final NamedClusterResolver namedClusterResolver;

  public ParquetOutputMeta() {
    this( new NamedClusterResolver( new NamedClusterServiceLocatorImpl( "", NamedClusterManager.getInstance() ), NamedClusterManager.getInstance() ) );
  }

  public ParquetOutputMeta( NamedClusterResolver namedClusterResolver ) {
    this.namedClusterResolver = namedClusterResolver;
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                                Trans trans ) {
    return new ParquetOutput( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new ParquetOutputData();
  }

  public NamedClusterResolver getNamedClusterResolver() {
    return namedClusterResolver;
  }
}
