/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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
