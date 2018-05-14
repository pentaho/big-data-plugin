/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2019-2019 by Hitachi Vantara : http://www.pentaho.com
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
package org.pentaho.big.data.kettle.plugins.formats.impl.avro.input;

import com.google.common.annotations.VisibleForTesting;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.big.data.kettle.plugins.formats.avro.input.AvroInputMetaBase;
import org.pentaho.big.data.kettle.plugins.formats.impl.NamedClusterResolver;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.osgi.api.MetastoreLocatorOsgi;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;

//keep ID as new because we will have old step with ID AvroInput
@Step( id = "AvroInputNew", image = "AI.svg", name = "AvroInput.Name", description = "AvroInput.Description",
  categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.BigData",
  documentationUrl = "Products/Avro_Input",
  i18nPackageName = "org.pentaho.di.trans.steps.avro" )
@InjectionSupported( localizationPrefix = "AvroInput.Injection.", groups = { "FIELDS", "LOOKUP_FIELDS" }, hide = {
  "FIELD_POSITION", "FIELD_LENGTH", "FIELD_IGNORE", "FIELD_FORMAT", "FIELD_PRECISION", "FIELD_CURRENCY",
  "FIELD_DECIMAL", "FIELD_GROUP", "FIELD_REPEAT", "FIELD_TRIM_TYPE"
} )
public class AvroInputMeta extends AvroInputMetaBase {

  private final NamedClusterServiceLocator namedClusterServiceLocator;
  private final NamedClusterService namedClusterService;
  private MetastoreLocatorOsgi metaStoreService;
  private Boolean testing = false;

  public AvroInputMeta( NamedClusterServiceLocator namedClusterServiceLocator,
                        NamedClusterService namedClusterService, MetastoreLocatorOsgi metaStore ) {
    this.namedClusterServiceLocator = namedClusterServiceLocator;
    this.namedClusterService = namedClusterService;
    this.metaStoreService = metaStore;
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                                Trans trans ) {
    return new AvroInput( stepMeta, stepDataInterface, copyNr, transMeta, trans, namedClusterServiceLocator );
  }

  @Override
  public StepDataInterface getStepData() {
    return new AvroInputData();
  }

  public NamedCluster getNamedCluster() {
    if ( !testing ) {
      NamedCluster namedCluster =
        NamedClusterResolver.resolveNamedCluster( namedClusterServiceLocator, namedClusterService, metaStoreService,
          this.getDataLocation() );
      return namedCluster;
    } else {
      namedClusterService.getClusterTemplate();
      return null;
    }
  }

  public NamedCluster getNamedCluster( String fileUri ) {
    NamedCluster namedCluster =
      NamedClusterResolver
        .resolveNamedCluster( namedClusterServiceLocator, namedClusterService, metaStoreService, fileUri );
    return namedCluster;
  }

  public NamedClusterServiceLocator getNamedClusterServiceLocator() {
    return namedClusterServiceLocator;
  }

  @VisibleForTesting
  void setTesting( Boolean isTesting ) {
    testing = isTesting;
  }
}
