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
