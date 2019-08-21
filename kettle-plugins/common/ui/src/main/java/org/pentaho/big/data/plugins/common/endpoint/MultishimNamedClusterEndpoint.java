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
package org.pentaho.big.data.plugins.common.endpoint;

import org.pentaho.big.data.plugins.common.ui.MultishimNamedCluster;
import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

public class MultishimNamedClusterEndpoint {
  private NamedClusterService namedClusterService;

  public MultishimNamedClusterEndpoint( NamedClusterService namedClusterService ) {
    this.namedClusterService = namedClusterService;
  }

  //http://localhost:9051/cxf/multishim/newNamedCluster?name=testName
  @GET
  @Path( "/newNamedCluster" )
  public Response newNamedCluster( @QueryParam( "name" ) String name ) {
    Spoon spoon = Spoon.getInstance();
    AbstractMeta meta = (AbstractMeta) spoon.getActiveMeta();
    MultishimNamedCluster cl = new MultishimNamedCluster( meta, spoon.getMetaStore(), this.namedClusterService );
    String result = cl.newNamedCluster( name );
    return Response.ok( result ).build();
  }
}
