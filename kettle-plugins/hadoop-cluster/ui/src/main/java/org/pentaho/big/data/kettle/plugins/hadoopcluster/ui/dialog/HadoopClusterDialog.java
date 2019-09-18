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
import org.pentaho.di.ui.core.dialog.ThinDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.platform.settings.ServerPort;
import org.pentaho.platform.settings.ServerPortRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class HadoopClusterDialog extends ThinDialog {

  private static final Image LOGO = GUIResource.getInstance().getImageLogoSmall();
  private static final String OSGI_SERVICE_PORT = "OSGI_SERVICE_PORT";
  private static final int OPTIONS = SWT.APPLICATION_MODAL | SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX;
  private static final String THIN_CLIENT_HOST = "THIN_CLIENT_HOST";
  private static final String THIN_CLIENT_PORT = "THIN_CLIENT_PORT";
  private static final String LOCALHOST = "localhost";

  public HadoopClusterDialog( Shell shell, int width, int height ) {
    super( shell, width, height );
  }


  public void open( String title ) {
    open( title, null );
  }

  public void open( String title, String connectionName ) {

    StringBuilder clientPath = new StringBuilder();
    clientPath.append( getClientPath() );
    if ( connectionName != null ) {
      //TODO: implement path for editting a cluster - if we support that...
    } else {
      clientPath.append( "#/hadoopcluster" );
    }
    super.createDialog( title, getRepoURL( clientPath.toString() ),
      OPTIONS, LOGO );
    super.dialog.setMinimumSize( 630, 630 );

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

  private static String getClientPath() {
    Properties properties = new Properties();
    try {
      InputStream inputStream = HadoopClusterDialog.class.getClassLoader().getResourceAsStream( "project.properties" );
      properties.load( inputStream );
    } catch ( IOException e ) {
      e.printStackTrace();
    }
    return properties.getProperty( "CLIENT_PATH" );
  }

  private static Integer getOsgiServicePort() {
    // if no service port is specified try getting it from
    ServerPort osgiServicePort = ServerPortRegistry.getPort( OSGI_SERVICE_PORT );
    if ( osgiServicePort != null ) {
      return osgiServicePort.getAssignedPort();
    }
    return null;
  }

  private static String getRepoURL( String path ) {
    String host;
    Integer port;
    try {
      host = getKettleProperty( THIN_CLIENT_HOST );
      port = Integer.valueOf( getKettleProperty( THIN_CLIENT_PORT ) );
    } catch ( Exception e ) {
      host = LOCALHOST;
      port = getOsgiServicePort();
    }
    return "http://" + host + ":" + port + path;
  }

  private static String getKettleProperty( String propertyName ) {
    // loaded in system properties at startup
    return System.getProperty( propertyName );
  }
}
