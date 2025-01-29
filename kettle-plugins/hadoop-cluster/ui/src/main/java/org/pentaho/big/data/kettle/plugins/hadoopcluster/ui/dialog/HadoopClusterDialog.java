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


package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog;

import org.eclipse.swt.SWT;
import org.eclipse.swt.browser.BrowserFunction;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.dialog.ThinDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.util.HelpUtils;
import org.pentaho.platform.settings.ServerPort;
import org.pentaho.platform.settings.ServerPortRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

import static org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints.HadoopClusterManager.STRING_NAMED_CLUSTERS;

public class HadoopClusterDialog extends ThinDialog {

  private static final Image LOGO = GUIResource.getInstance().getImageLogoSmall();
  private static final String OSGI_SERVICE_PORT = "OSGI_SERVICE_PORT";
  private static final int OPTIONS = SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX;
  private static final String THIN_CLIENT_HOST = "THIN_CLIENT_HOST";
  private static final String THIN_CLIENT_PORT = "THIN_CLIENT_PORT";
  private static final String LOCALHOST = "127.0.0.1";

  private Supplier<Spoon> spoonSupplier = Spoon::getInstance;

  private final LogChannelInterface log = spoonSupplier.get().getLog();

  HadoopClusterDialog( Shell shell, int width, int height ) {
    super( shell, width, height, true );
  }

  void open( String title, String thinAppState, Map<String, String> urlParams ) {

    StringBuilder clientPath = new StringBuilder();
    clientPath.append( getClientPath() );
    clientPath.append( "#!/" );
    if ( thinAppState != null ) {
      clientPath.append( thinAppState );
    }

    //Convert map into url params string
    HashMap<String, String> params = new HashMap<>( urlParams );
    params.put( "connectedToRepo", Boolean.toString( connectedToRepo() ) );
    final String paramString = params.entrySet().stream()
      .map( p -> p.getKey() + "=" + p.getValue() )
      .reduce( ( p1, p2 ) -> p1 + "&" + p2 )
      .map( s -> "?" + s )
      .orElse( "" );

    clientPath.append( paramString );
    String endpointURL = getEndpointURL( clientPath.toString() );
    log.logDebug( "Thin endpoint URL:  " + endpointURL );
    super.createDialog( title, endpointURL, OPTIONS, LOGO );
    super.dialog.setMinimumSize( 640, 630 );

    new BrowserFunction( browser, "open" ) {
      @Override public Object function( Object[] arguments ) {
        HelpUtils.openHelpDialog( spoonSupplier.get().getDisplay().getActiveShell(), "", (String) arguments[ 0 ], "" );
        return true;
      }
    };

    new BrowserFunction( browser, "close" ) {
      @Override public Object function( Object[] arguments ) {
        Runnable execute = () -> {
          browser.dispose();
          dialog.close();
          dialog.dispose();
        };
        display.asyncExec( execute );
        return true;
      }
    };

    new BrowserFunction( browser, "setTitle" ) {
      @Override public Object function( Object[] arguments ) {
        Runnable execute = () -> {
          dialog.setText( (String) arguments[ 0 ] );
        };
        display.asyncExec( execute );
        return true;
      }
    };

    while ( !dialog.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    Spoon spoon = spoonSupplier.get();
    if ( spoon != null && spoon.getShell() != null ) {
      spoon.getShell().getDisplay().asyncExec( () -> spoon.refreshTree( STRING_NAMED_CLUSTERS ) );
    }
  }

  private String getClientPath() {
    Properties properties = new Properties();
    try {
      InputStream inputStream = HadoopClusterDialog.class.getClassLoader().getResourceAsStream( "project.properties" );
      properties.load( inputStream );
    } catch ( IOException e ) {
      log.logError( e.getMessage(), e );
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
    if ( Const.isRunningOnWebspoonMode() ) {
      return System.getProperty( "KETTLE_CONTEXT_PATH", "" ) + "/osgi" + path;
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
