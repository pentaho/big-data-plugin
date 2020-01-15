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

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileItemFactory;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.json.simple.JSONObject;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.HadoopClusterDialog;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.model.ThinNameClusterModel;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.osgi.metastore.locator.api.MetastoreLocator;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.runtime.test.RuntimeTester;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.pentaho.di.ui.util.HelpUtils;

public class HadoopClusterEndpoints {

  private static final Class<?> PKG = HadoopClusterDialog.class;
  private final Supplier<Spoon> spoonSupplier = Spoon::getInstance;
  private final NamedClusterService namedClusterService;
  private final MetastoreLocator metastoreLocator;
  private final RuntimeTester runtimeTester;
  private final String internalShim;
  private final boolean secureEnabled;

  private enum FILE_TYPE {
    CONFIGURATION( "configuration" ),
    DRIVER( ".kar" );

    private String val;

    FILE_TYPE( String val ) {
      this.val = val;
    }

    public String getValue() {
      return this.val;
    }
  }

  public HadoopClusterEndpoints( MetastoreLocator metastoreLocator, NamedClusterService namedClusterService,
                                 RuntimeTester runtimeTester, String internalShim, boolean secureEnabled ) {
    this.namedClusterService = namedClusterService;
    this.metastoreLocator = metastoreLocator;
    this.runtimeTester = runtimeTester;
    this.internalShim = internalShim;
    this.secureEnabled = secureEnabled;
  }

  private HadoopClusterManager getClusterManager() {
    return new HadoopClusterManager( spoonSupplier.get(), this.namedClusterService,
      this.metastoreLocator.getMetastore(),
      internalShim );
  }

  private List<FileItem> parseRequest( HttpServletRequest request, FILE_TYPE fileType ) {
    List<FileItem> files = new ArrayList<>();
    if ( ServletFileUpload.isMultipartContent( request ) ) {
      try {
        FileItemFactory factory = new DiskFileItemFactory();
        ServletFileUpload fileUpload = new ServletFileUpload( factory );
        List<FileItem> fileItems = fileUpload.parseRequest( request );
        for ( FileItem fileItem : fileItems ) {
          validateUpload( fileItem, fileType, files );
        }
      } catch ( FileUploadException e ) {
        files = new ArrayList<>();
      }
    }
    return files;
  }

  private void validateUpload( FileItem fileItem, FILE_TYPE fileType, List<FileItem> files ) {
    if (
      ( fileType.equals( FILE_TYPE.CONFIGURATION ) && getClusterManager().isValidConfigurationFile( fileItem ) )
        ||
        ( fileType.equals( FILE_TYPE.DRIVER ) && fileItem.getFieldName().endsWith( FILE_TYPE.DRIVER.getValue() ) )
    ) {
      files.add( fileItem );
    }
  }

  //http://localhost:9051/cxf/hadoop-cluster/importNamedCluster
  @POST
  @Consumes( { MediaType.MULTIPART_FORM_DATA } )
  @Path( "/importNamedCluster" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public Response importNamedCluster( @Context HttpServletRequest request ) {
    List<FileItem> siteFilesSource = parseRequest( request, FILE_TYPE.CONFIGURATION );
    ThinNameClusterModel model = ThinNameClusterModel.unmarshall( siteFilesSource );
    JSONObject response = getClusterManager().importNamedCluster( model, siteFilesSource );
    return Response.ok( response ).build();
  }

  //http://localhost:9051/cxf/hadoop-cluster/createNamedCluster
  @POST
  @Path( "/createNamedCluster" )
  @Consumes( { MediaType.MULTIPART_FORM_DATA } )
  @Produces( { MediaType.APPLICATION_JSON } )
  public Response createNamedCluster( @Context HttpServletRequest request ) {
    List<FileItem> siteFilesSource = parseRequest( request, FILE_TYPE.CONFIGURATION );
    ThinNameClusterModel model = ThinNameClusterModel.unmarshall( siteFilesSource );
    JSONObject response = getClusterManager().createNamedCluster( model, siteFilesSource );
    return Response.ok( response ).build();
  }

  //http://localhost:9051/cxf/hadoop-cluster/editNamedCluster
  @POST
  @Path( "/editNamedCluster" )
  @Consumes( { MediaType.MULTIPART_FORM_DATA } )
  @Produces( { MediaType.APPLICATION_JSON } )
  public Response editNamedCluster( @Context HttpServletRequest request ) {
    List<FileItem> siteFilesSource = parseRequest( request, FILE_TYPE.CONFIGURATION );
    ThinNameClusterModel model = ThinNameClusterModel.unmarshall( siteFilesSource );
    JSONObject response = getClusterManager().editNamedCluster( model, true, siteFilesSource );
    return Response.ok( response ).build();
  }

  //http://localhost:9051/cxf/hadoop-cluster/duplicateNamedCluster
  @POST
  @Path( "/duplicateNamedCluster" )
  @Consumes( { MediaType.MULTIPART_FORM_DATA } )
  @Produces( { MediaType.APPLICATION_JSON } )
  public Response duplicateNamedCluster( @Context HttpServletRequest request ) {
    List<FileItem> siteFilesSource = parseRequest( request, FILE_TYPE.CONFIGURATION );
    ThinNameClusterModel model = ThinNameClusterModel.unmarshall( siteFilesSource );
    JSONObject response = getClusterManager().editNamedCluster( model, false, siteFilesSource );
    return Response.ok( response ).build();
  }

  //http://localhost:9051/cxf/hadoop-cluster/getNamedCluster?namedCluster=
  @GET
  @Path( "/getNamedCluster" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public Response getNamedCluster( @QueryParam( "namedCluster" ) String namedCluster ) {
    return Response.ok( getClusterManager().getNamedCluster( namedCluster ) ).build();
  }

  //http://localhost:9051/cxf/hadoop-cluster/getShimIdentifiers
  @GET
  @Path( "/getShimIdentifiers" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public Response getShimIdentifiers() {
    return Response.ok( getClusterManager().getShimIdentifiers() ).build();
  }

  //http://localhost:9051/cxf/hadoop-cluster/runTests?namedCluster=
  @GET
  @Path( "/runTests" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public Response runTests( @QueryParam( "namedCluster" ) String namedCluster ) {
    return Response.ok( getClusterManager().runTests( runtimeTester, namedCluster ) ).build();
  }

  //http://localhost:9051/cxf/hadoop-cluster/installDriver
  @POST
  @Path( "/installDriver" )
  @Consumes( { MediaType.MULTIPART_FORM_DATA } )
  @Produces( { MediaType.APPLICATION_JSON } )
  public Response installDriver( @Context HttpServletRequest request ) {
    List<FileItem> driver = parseRequest( request, FILE_TYPE.DRIVER );
    return Response.ok( getClusterManager().installDriver( driver ) ).build();
  }

  //http://localhost:9051/cxf/hadoop-cluster/getSecure
  @GET
  @Path( "/getSecure" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public Response getSecure() {
    return Response.ok( "{\"secureEnabled\":\"" + Boolean.toString( this.secureEnabled ) + "\"}" ).build();
  }
}
