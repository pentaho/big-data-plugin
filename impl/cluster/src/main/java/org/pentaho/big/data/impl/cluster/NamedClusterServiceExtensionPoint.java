/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/
package org.pentaho.big.data.impl.cluster;

import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.osgi.api.NamedClusterServiceOsgi;
import org.pentaho.di.job.JobExecutionExtension;
import org.pentaho.di.trans.Trans;

/**
 * Created by tkafalas on 7/14/2017.
 */
@ExtensionPoint( id = "NamedClusterServiceMetaLoadExtensionPoint", extensionPointId = "TransformationMetaLoaded",
  description = "" )
public class NamedClusterServiceExtensionPoint implements ExtensionPointInterface {
  NamedClusterServiceOsgi namedClusterServiceOsgi;

  public NamedClusterServiceExtensionPoint( NamedClusterService namedClusterService ) {
    namedClusterServiceOsgi = new NamedClusterServiceOsgiImpl( namedClusterService );
  }

  @Override public void callExtensionPoint( LogChannelInterface log, Object object ) throws KettleException {
    AbstractMeta meta;
    if ( object instanceof Trans ) {
      meta = ( (Trans) object ).getTransMeta();
    } else if ( object instanceof JobExecutionExtension ) {
      meta = ( (JobExecutionExtension) object ).job.getJobMeta();
    } else {
      meta = (AbstractMeta) object;
    }
    meta.setNamedClusterServiceOsgi( namedClusterServiceOsgi );
  }
}
