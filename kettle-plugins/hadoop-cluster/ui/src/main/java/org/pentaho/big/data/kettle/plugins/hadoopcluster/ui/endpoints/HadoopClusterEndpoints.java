/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints;

import org.json.simple.JSONObject;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.HadoopClusterDialog;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.model.ThinNameClusterModel;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.runtime.test.RuntimeTester;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.function.Supplier;

import org.pentaho.di.ui.util.HelpUtils;

public class HadoopClusterEndpoints {

  private static final Class<?> PKG = HadoopClusterDialog.class;
  private Supplier<Spoon> spoonSupplier = Spoon::getInstance;
  private NamedClusterService namedClusterService;
  private RuntimeTester runtimeTester;

  public static final String
      HELP_URL =
      Const.getDocUrl( BaseMessages.getString( PKG, "HadoopCluster.help.dialog.Help" ) );

  public HadoopClusterEndpoints( NamedClusterService namedClusterService, RuntimeTester runtimeTester ) {
    this.namedClusterService = namedClusterService;
    this.runtimeTester = runtimeTester;
  }

  @GET @Path( "/help" ) public Response help() {
    spoonSupplier.get().getShell().getDisplay().asyncExec( () -> HelpUtils
        .openHelpDialog( spoonSupplier.get().getDisplay().getActiveShell(),
            BaseMessages.getString( PKG, "HadoopCluster.help.dialog.Title" ), HELP_URL,
            BaseMessages.getString( PKG, "HadoopCluster.help.dialog.Header" ) ) );
    return Response.ok().build();
  }

  //http://localhost:9051/cxf/hadoop-cluster/importNamedCluster
  @POST @Path( "/importNamedCluster" ) @Produces( { MediaType.APPLICATION_JSON } ) public Response importNamedCluster(
      ThinNameClusterModel model ) {
    HadoopClusterManager
        hadoopClusterManager =
        new HadoopClusterManager( spoonSupplier.get(), this.namedClusterService );
    JSONObject response = hadoopClusterManager.importNamedCluster( model );
    return Response.ok( response ).build();
  }

  //http://localhost:9051/cxf/hadoop-cluster/createNamedCluster
  @POST @Path( "/createNamedCluster" ) @Consumes( { MediaType.APPLICATION_JSON } )
  @Produces( { MediaType.APPLICATION_JSON } ) public Response createNamedCluster( ThinNameClusterModel model ) {
    HadoopClusterManager
        hadoopClusterManager =
        new HadoopClusterManager( spoonSupplier.get(), this.namedClusterService );
    JSONObject response = hadoopClusterManager.createNamedCluster( model );
    return Response.ok( response ).build();
  }

  //http://localhost:9051/cxf/hadoop-cluster/getNamedCluster?namedCluster=
  @GET @Path( "/getNamedCluster" ) @Produces( { MediaType.APPLICATION_JSON } ) public Response getNamedCluster(
      @QueryParam( "namedCluster" ) String namedCluster ) {
    HadoopClusterManager
        hadoopClusterManager =
        new HadoopClusterManager( spoonSupplier.get(), this.namedClusterService );
    return Response.ok( hadoopClusterManager.getNamedCluster( namedCluster ) ).build();
  }

  //http://localhost:9051/cxf/hadoop-cluster/getShimIdentifiers
  @GET @Path( "/getShimIdentifiers" ) @Produces( { MediaType.APPLICATION_JSON } ) public Response getShimIdentifiers() {
    HadoopClusterManager
        hadoopClusterManager =
        new HadoopClusterManager( spoonSupplier.get(), this.namedClusterService );
    return Response.ok( hadoopClusterManager.getShimIdentifiers() ).build();
  }

  //http://localhost:9051/cxf/hadoop-cluster/runTests?namedCluster=
  @GET @Path( "/runTests" ) @Produces( { MediaType.APPLICATION_JSON } ) public Response runTests(
      @QueryParam( "namedCluster" ) String namedCluster ) {
    HadoopClusterManager
        hadoopClusterManager =
        new HadoopClusterManager( spoonSupplier.get(), this.namedClusterService );
    return Response.ok( hadoopClusterManager.runTests( runtimeTester, namedCluster ) ).build();
  }
}
