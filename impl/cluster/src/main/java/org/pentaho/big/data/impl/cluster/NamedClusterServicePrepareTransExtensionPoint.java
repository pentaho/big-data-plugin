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
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.osgi.api.NamedClusterServiceOsgi;


/**
 * Created by tkafalas on 7/14/2017.
 *  * <p>
 * This class exists because two ExtensionPoint annotations are not allowed on the same class
 */
@ExtensionPoint( id = "NamedClusterServicePrepareTransExtensionPoint", extensionPointId = "TransformationPrepareExecution",
  description = "" )
public class NamedClusterServicePrepareTransExtensionPoint extends NamedClusterServiceExtensionPoint {
  NamedClusterServiceOsgi namedClusterServiceOsgi;

  public NamedClusterServicePrepareTransExtensionPoint( NamedClusterService namedClusterService ) {
    super( namedClusterService );
  }
}
