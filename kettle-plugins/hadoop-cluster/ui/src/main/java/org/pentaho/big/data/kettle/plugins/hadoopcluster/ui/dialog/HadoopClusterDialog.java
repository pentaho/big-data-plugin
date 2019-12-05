/*******************************************************************************
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

package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog;

import org.eclipse.swt.SWT;
import org.eclipse.swt.browser.BrowserFunction;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.repository.IUser;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.dialog.ThinDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.platform.settings.ServerPort;
import org.pentaho.platform.settings.ServerPortRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

public class HadoopClusterDialog extends ThinDialog {

  private static final Image LOGO = GUIResource.getInstance().getImageLogoSmall();
  private static final String OSGI_SERVICE_PORT = "OSGI_SERVICE_PORT";
  private static final int OPTIONS = SWT.APPLICATION_MODAL | SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX;
  private static final String THIN_CLIENT_HOST = "THIN_CLIENT_HOST";
  private static final String THIN_CLIENT_PORT = "THIN_CLIENT_PORT";
  private static final String LOCALHOST = "localhost";

  private Supplier<Spoon> spoonSupplier = Spoon::getInstance;

  private static final LogChannelInterface LOG = LogChannel.GENERAL;

  HadoopClusterDialog( Shell shell, int width, int height ) {
    super( shell, width, height );
  }

  void open( String title, String thinAppState, Map<String, String> urlParams ) {

    StringBuilder clientPath = new StringBuilder();
    clientPath.append( getClientPath() );
    clientPath.append( "#/" );
    if ( thinAppState != null ) {
      clientPath.append( thinAppState );
    }

    //Convert map into url params string
    final String paramString = urlParams.entrySet().stream()
      .map( p -> p.getKey() + "=" + p.getValue() )
      .reduce( ( p1, p2 ) -> p1 + "&" + p2 )
      .map( s -> "?" + s )
      .orElse( "" );

    clientPath.append( paramString );
    String endpointURL = getEndpointURL( clientPath.toString() );
    LOG.logDebug( "Thin endpoint URL:  " + endpointURL );
    super.createDialog( title, endpointURL, OPTIONS, LOGO );
    super.dialog.setMinimumSize( 640, 630 );

    if ( connectedToRepo() ) {
      IUser userInfo = getRepo().getUserInfo();
      LOG.logDebug( "Connecting to endpoint with user " + userInfo.getName() );
      String auth = userInfo.getName() + ":" + userInfo.getPassword();
      String encodedAuth = Base64.getEncoder().encodeToString( auth.getBytes() );
      browser.setUrl( endpointURL, null, new String[] { "Authorization: Basic " + encodedAuth } );
    }


    new BrowserFunction( browser, "close" ) {
      @Override public Object function( Object[] arguments ) {
        browser.dispose();
        dialog.close();
        dialog.dispose();
        return true;
      }
    };

    new BrowserFunction( browser, "setTitle" ) {
      @Override public Object function( Object[] arguments ) {
        dialog.setText( (String) arguments[ 0 ] );
        return true;
      }
    };

    new BrowserFunction( browser, "browse" ) {
      @Override public Object function( Object[] arguments ) {
        String browseType = (String) arguments[ 0 ];
        String startPath = (String) arguments[ 1 ];

        if ( "folder".equals( browseType ) ) {
          DirectoryDialog folderDialog = new DirectoryDialog( getParent().getShell(), SWT.OPEN );
          folderDialog.setFilterPath( startPath );
          return folderDialog.open();
        } else {
          FileDialog fileDialog = new FileDialog( getParent().getShell(), SWT.OPEN );
          fileDialog.setFileName( startPath );
          return fileDialog.open();
        }
      }
    };

    while ( !dialog.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
  }

  private String getClientPath() {
    Properties properties = new Properties();
    try {
      InputStream inputStream = HadoopClusterDialog.class.getClassLoader().getResourceAsStream( "project.properties" );
      properties.load( inputStream );
    } catch ( IOException e ) {
      LOG.logError( e.getMessage(), e );
    }
    return properties.getProperty( "CLIENT_PATH" );
  }

  private int getOsgiServicePort() {
    // if no service port is specified try getting it from
    ServerPort osgiServicePort = ServerPortRegistry.getPort( OSGI_SERVICE_PORT );
    if ( osgiServicePort != null ) {
      return osgiServicePort.getAssignedPort();
    }
    throw new IllegalStateException( "No osgi service port defined" );
  }

  private String getEndpointURL( String path ) {
    if ( connectedToRepo() ) {
      return getRepo().getUri()
        .orElseThrow( () -> new IllegalStateException( "Repo URI not defined" ) )
        .toString() + "/osgi" + path;
    }
    String host;
    int port;
    try {
      host = getKettleProperty( THIN_CLIENT_HOST );
      port = Integer.parseInt( getKettleProperty( THIN_CLIENT_PORT ) );
    } catch ( Exception e ) {
      host = LOCALHOST;
      port = getOsgiServicePort();
    }
    return "http://" + host + ":" + port + path;
  }

  private boolean connectedToRepo() {
    Repository repo = getRepo();
    return repo != null && repo.getUri().isPresent();
  }

  private Repository getRepo() {
    return spoonSupplier.get().getRepository();
  }

  private String getKettleProperty( String propertyName ) {
    // loaded in system properties at startup
    return System.getProperty( propertyName );
  }
}
