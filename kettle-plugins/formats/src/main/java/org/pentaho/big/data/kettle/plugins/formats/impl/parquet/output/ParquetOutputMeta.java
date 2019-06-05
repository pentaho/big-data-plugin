/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018-2019 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.big.data.kettle.plugins.formats.impl.parquet.output;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
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
  documentationUrl = "Products/Parquet_Output",
  i18nPackageName = "org.pentaho.di.trans.steps.parquet" )
@InjectionSupported( localizationPrefix = "ParquetOutput.Injection.", groups = { "FILENAME_LINES", "FIELDS" }, hide = {
  "FIELD_POSITION", "FIELD_LENGTH", "FIELD_IGNORE", "FIELD_FORMAT", "FIELD_PRECISION", "FIELD_CURRENCY",
  "FIELD_DECIMAL", "FIELD_GROUP", "FIELD_REPEAT", "FIELD_TRIM_TYPE", "FIELD_NULL_STRING"
} )
public class ParquetOutputMeta extends ParquetOutputMetaBase {

  private final NamedClusterServiceLocator namedClusterServiceLocator;
  private final NamedClusterService namedClusterService;

  public ParquetOutputMeta( NamedClusterServiceLocator namedClusterServiceLocator,
                            NamedClusterService namedClusterService ) {
    this.namedClusterServiceLocator = namedClusterServiceLocator;
    this.namedClusterService = namedClusterService;
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                                Trans trans ) {
    return new ParquetOutput( stepMeta, stepDataInterface, copyNr, transMeta, trans, namedClusterServiceLocator );
  }

  public NamedCluster getNamedCluster() {
    return namedClusterService.getClusterTemplate();
  }

  @Override
  public StepDataInterface getStepData() {
    return new ParquetOutputData();
  }

}
