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


package org.pentaho.big.data.kettle.plugins.pig;

import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.big.data.impl.cluster.NamedClusterImpl;
import org.pentaho.di.trans.steps.loadsave.validator.FieldLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.StringLoadSaveValidator;

/**
 * Created by bryan on 10/19/15.
 */
public class PigNamedClusterValidator implements FieldLoadSaveValidator<NamedCluster> {
  private final StringLoadSaveValidator stringLoadSaveValidator = new StringLoadSaveValidator();

  @Override public NamedCluster getTestObject() {
    NamedClusterImpl namedCluster = new NamedClusterImpl();
    namedCluster.setHdfsHost( stringLoadSaveValidator.getTestObject() );
    namedCluster.setHdfsPort( stringLoadSaveValidator.getTestObject() );
    namedCluster.setJobTrackerHost( stringLoadSaveValidator.getTestObject() );
    namedCluster.setJobTrackerPort( stringLoadSaveValidator.getTestObject() );
    return namedCluster;
  }

  /**
   * Only cares about hdfs host, port, jobtracker host, port
   *
   * @param namedCluster
   * @param o
   * @return
   */
  @Override public boolean validateTestObject( NamedCluster namedCluster, Object o ) {
    if ( o instanceof NamedCluster ) {
      NamedCluster namedCluster2 = (NamedCluster) o;
      return stringLoadSaveValidator.validateTestObject( namedCluster.getHdfsHost(), namedCluster2.getHdfsHost() )
        && stringLoadSaveValidator.validateTestObject( namedCluster.getHdfsPort(), namedCluster2.getHdfsPort() )
        && stringLoadSaveValidator.validateTestObject( namedCluster.getJobTrackerHost(),
        namedCluster2.getJobTrackerHost() )
        && stringLoadSaveValidator.validateTestObject( namedCluster.getJobTrackerPort(),
        namedCluster2.getJobTrackerPort() );
    }
    return false;
  }
}
