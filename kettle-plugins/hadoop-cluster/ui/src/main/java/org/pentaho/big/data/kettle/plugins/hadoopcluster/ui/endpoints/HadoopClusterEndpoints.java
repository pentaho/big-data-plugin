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

import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.HadoopClusterDialog;
import org.pentaho.di.base.AbstractMeta;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.osgi.metastore.locator.api.MetastoreLocator;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import java.util.function.Supplier;

import org.pentaho.di.ui.util.HelpUtils;
import org.pentaho.big.data.plugins.common.ui.MultishimNamedCluster;

public class HadoopClusterEndpoints {

  private static Class<?> PKG = HadoopClusterDialog.class;
  private Supplier<Spoon> spoonSupplier = Spoon::getInstance;
  private NamedClusterService namedClusterService;

  public static final String HELP_URL =
    Const.getDocUrl( BaseMessages.getString( PKG, "HadoopCluster.help.dialog.Help" ) );

  public HadoopClusterEndpoints( MetastoreLocator metastoreLocator, NamedClusterService namedClusterService ) {
    this.namedClusterService = namedClusterService;
  }

  @GET
  @Path( "/help" )
  public Response help() {
    spoonSupplier.get().getShell().getDisplay().asyncExec( () ->
      HelpUtils.openHelpDialog( spoonSupplier.get().getDisplay().getActiveShell(),
        BaseMessages.getString( PKG, "HadoopCluster.help.dialog.Title" ),
        HELP_URL, BaseMessages.getString( PKG, "HadoopCluster.help.dialog.Header" ) ) );
    return Response.ok().build();
  }

  //http://localhost:9051/cxf/hadoop-cluster/newNamedCluster?name=testName
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
