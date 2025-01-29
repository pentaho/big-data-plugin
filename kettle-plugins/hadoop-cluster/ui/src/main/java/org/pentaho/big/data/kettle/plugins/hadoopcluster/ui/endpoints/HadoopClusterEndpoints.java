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


package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.fileupload.FileItemIterator;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.IOUtils;
import org.json.simple.JSONObject;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.model.ThinNameClusterModel;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.service.PluginServiceLoader;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.metastore.locator.api.MetastoreLocator;
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
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class HadoopClusterEndpoints {
  private static final LogChannelInterface log =
    KettleLogStore.getLogChannelInterfaceFactory().create( "HadoopClusterEndpoints" );
  private final Supplier<Spoon> spoonSupplier = Spoon::getInstance;
  private final NamedClusterService namedClusterService;
  private MetastoreLocator metastoreLocator;
  private final RuntimeTester runtimeTester;
  private final String internalShim;
  private final boolean secureEnabled;
  private static final String MOD_DATE_FILENAME_PREFIX = "mod-";
  private static final String FILE_CONTENT_FILENAME_PREFIX = "file-";
  private static final String ZERO = "0";

  enum FileType {
    CONFIGURATION( "configuration" ),
    DRIVER( ".kar" );

    private String val;

    FileType( String val ) {
      this.val = val;
    }

    String getValue() {
      return this.val;
    }
  }

  public HadoopClusterEndpoints( NamedClusterService namedClusterService,
                                 RuntimeTester runtimeTester, String internalShim, boolean secureEnabled ) {
    this.namedClusterService = namedClusterService;
    this.runtimeTester = runtimeTester;
    this.internalShim = internalShim;
    this.secureEnabled = secureEnabled;
  }

  protected synchronized MetastoreLocator getMetastoreLocator() {
    if ( this.metastoreLocator == null ) {
      try {
        Collection<MetastoreLocator> metastoreLocators = PluginServiceLoader.loadServices( MetastoreLocator.class );
        this.metastoreLocator = metastoreLocators.stream().findFirst().get();
      } catch ( Exception e ) {
        log.logError( "Error getting MetastoreLocator", e );
      }
    }
    return this.metastoreLocator;
  }

  private HadoopClusterManager getClusterManager() {
    return new HadoopClusterManager( spoonSupplier.get(), this.namedClusterService,
    getMetastoreLocator().getMetastore(),
      internalShim );
  }

  private Map<String, CachedFileItemStream> parseRequest( HttpServletRequest request, FileType fileType ) {
    Map<String, CachedFileItemStream> fileStreamByName = new HashMap<>();
    if ( ServletFileUpload.isMultipartContent( request ) ) {
      try {
        CachedFileItemStream lastCachedFileInputStream =
          null; //Holds the last cached item reference so can be accessed to add a modification date
        FileItemIterator streamItemIterator = ( new ServletFileUpload() ).getItemIterator( request );
        while ( streamItemIterator.hasNext() ) {
          FileItemStream fileItemStream = streamItemIterator.next();
          if ( fileItemStream.getFieldName().startsWith( MOD_DATE_FILENAME_PREFIX ) ) {
            //We have a modification date coming date
            String realFileName = removePrefix( fileItemStream.getFieldName(), MOD_DATE_FILENAME_PREFIX );
            if ( lastCachedFileInputStream != null && realFileName.equals( lastCachedFileInputStream.getFieldName() ) ) {
              String millis = ZERO;
              try ( InputStream is = fileItemStream.openStream() ) {
                millis = IOUtils.toString( fileItemStream.openStream(), String.valueOf( StandardCharsets.UTF_8 ) );
              }
              long modificationDateMillis = Long.parseLong( millis );
              lastCachedFileInputStream.setLastModified( modificationDateMillis );
            }
          } else {
            //We have file content coming in
            String realFileName = removePrefix( fileItemStream.getFieldName(), FILE_CONTENT_FILENAME_PREFIX );
            List<CachedFileItemStream> fileItemStreams = copyAndUnzip( fileItemStream, fileType, realFileName );
            for ( CachedFileItemStream cachedFileItemStream : fileItemStreams ) {
              fileStreamByName.put( cachedFileItemStream.getFieldName(), cachedFileItemStream );
              lastCachedFileInputStream = cachedFileItemStream;
            }
          }

        }
      } catch ( FileUploadException | IOException e ) {
        log.logError( e.getMessage() );
      }
    }
    return fileStreamByName;
  }

  private String removePrefix( String fieldName, String prefix ) {
    return fieldName.startsWith( prefix ) ? fieldName.substring( prefix.length() ) :  fieldName;
  }

  private boolean isValidUpload( String fileName, FileType fileType ) {
    boolean valid = false;
    if (
      ( fileType.equals( FileType.CONFIGURATION ) && getClusterManager().isValidConfigurationFile( fileName ) )
        ||
        ( fileType.equals( FileType.DRIVER ) && fileName.endsWith( FileType.DRIVER.getValue() ) )
    ) {
      valid = true;
    }
    return valid;
  }

  /**
   * Copy and Unzip
   * <p>
   * Copies a {@link FileItemStream} to a {@link List} of {@link CachedFileItemStream}s. A single {@link FileItemStream}
   * may be zipped. This method unzips the zipped stream and copies each unzipped file to their own {@link
   * CachedFileItemStream}
   *
   * @param fileItemStream the file item stream to unzip (if zipped) and copy
   * @return a {@link List} of {@link CachedFileItemStream}s
   * @throws IOException
   */
  @VisibleForTesting
  List<CachedFileItemStream> copyAndUnzip( FileItemStream fileItemStream, FileType fileType, String realFileName )
    throws IOException {

    List<CachedFileItemStream> unzippedFileItemStreams = new ArrayList<>();

    if ( realFileName.endsWith( ".zip" ) ) {

      try ( ZipInputStream zis = new ZipInputStream( fileItemStream.openStream() ) ) {

        for ( ZipEntry zipEntry = zis.getNextEntry(); zipEntry != null; zipEntry = zis.getNextEntry() ) {
          if ( !zipEntry.isDirectory() ) {
            //remove all directory structure from the zip file names and only unzip the files
            String[] split = zipEntry.getName().split( "/" ); //zip files always use forward slash
            String unzippedFileName = split[ split.length - 1 ];
            if ( isValidUpload( unzippedFileName, fileType ) ) {
              CachedFileItemStream unzippedFileItemStream =
                new CachedFileItemStream( zis, fileItemStream.getName(), unzippedFileName );
              unzippedFileItemStream.setLastModified( zipEntry.getLastModifiedTime().toMillis() );
              unzippedFileItemStreams.add( unzippedFileItemStream );
            }
          }
        }
      }
    } else {
      //file is not zipped
      if ( isValidUpload( realFileName, fileType ) ) {
        unzippedFileItemStreams.add( new CachedFileItemStream( fileItemStream.openStream(), fileItemStream.getName(),
          realFileName ) );
      }
    }
    return unzippedFileItemStreams;
  }

  //http://localhost:9051/cxf/hadoop-cluster/importNamedCluster
  @POST
  @Consumes( { MediaType.MULTIPART_FORM_DATA } )
  @Path( "/importNamedCluster" )
  @Produces( { MediaType.APPLICATION_JSON } )
  public Response importNamedCluster( @Context HttpServletRequest request ) {
    Map<String, CachedFileItemStream> siteFilesSource = parseRequest( request, FileType.CONFIGURATION );
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
    Map<String, CachedFileItemStream> siteFilesSource = parseRequest( request, FileType.CONFIGURATION );
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
    Map<String, CachedFileItemStream> siteFilesSource = parseRequest( request, FileType.CONFIGURATION );
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
    Map<String, CachedFileItemStream> siteFilesSource = parseRequest( request, FileType.CONFIGURATION );
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
    FileItemStream driver = null;
    if ( ServletFileUpload.isMultipartContent( request ) ) {
      try {
        FileItemIterator streamItemIterator = ( new ServletFileUpload() ).getItemIterator( request );
        if ( streamItemIterator.hasNext() ) {
          FileItemStream fileItemStream = streamItemIterator.next();
          if ( isValidUpload( fileItemStream.getFieldName(), FileType.DRIVER ) ) {
            driver = fileItemStream;
          }
        }
      } catch ( FileUploadException | IOException e ) {
        log.logError( e.getMessage() );
      }
    }
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
