/*!
 * HITACHI VANTARA PROPRIETARY AND CONFIDENTIAL
 *
 * Copyright 2020 Hitachi Vantara. All rights reserved.
 *
 * NOTICE: All information including source code contained herein is, and
 * remains the sole property of Hitachi Vantara and its licensors. The intellectual
 * and technical concepts contained herein are proprietary and confidential
 * to, and are trade secrets of Hitachi Vantara and may be covered by U.S. and foreign
 * patents, or patents in process, and are protected by trade secret and
 * copyright laws. The receipt or possession of this source code and/or related
 * information does not convey or imply any rights to reproduce, disclose or
 * distribute its contents, or to manufacture, use, or sell anything that it
 * may describe, in whole or in part. Any reproduction, modification, distribution,
 * or public display of this information without the express written authorization
 * from Hitachi Vantara is strictly prohibited and in violation of applicable laws and
 * international treaties. Access to the source code contained herein is strictly
 * prohibited to anyone except those individuals and entities who have executed
 * confidentiality and non-disclosure agreements or other agreements with Hitachi Vantara,
 * explicitly covering such access.
 */

package com.pentaho.di.plugins.catalog.common;

import com.pentaho.di.plugins.catalog.api.CatalogClient;
import com.pentaho.di.plugins.catalog.api.entities.DataResource;
import com.pentaho.di.plugins.catalog.api.entities.search.FacetsResult;
import com.pentaho.di.plugins.catalog.provider.CatalogDetails;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.connections.ConnectionManager;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.core.gui.GUIResource;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.function.Supplier;

public class DialogUtils {


  public static Image getImage( StepMeta stepMeta, Shell shell ) {
    PluginInterface plugin = PluginRegistry.getInstance().getPlugin( StepPluginType.class, stepMeta.getStepMetaInterface() );
    String id = plugin.getIds()[ 0 ];
    if ( id != null ) {
      return GUIResource.getInstance().getImagesSteps().get( id ).getAsBitmapForSize( shell.getDisplay(), ConstUI.ICON_SIZE, ConstUI.ICON_SIZE );
    }
    return null;
  }

  public static DataResource getDataResource( String id, Supplier<ConnectionManager> connManagerSupplier, CCombo wConnection, TransMeta transMeta ) {

    CatalogDetails catalogDetails = (CatalogDetails) connManagerSupplier.get().getConnectionDetails( CatalogDetails.CATALOG, wConnection.getText() );

    URL url;
    try {
      url = new URL( transMeta.environmentSubstitute( catalogDetails.getUrl() ) );
    } catch ( MalformedURLException mue ) {
      return null;
    }

    String username = transMeta.environmentSubstitute( catalogDetails.getUsername() );
    String password = transMeta.environmentSubstitute( catalogDetails.getPassword() );

    CatalogClient catalogClient = new CatalogClient( url.getHost(), String.valueOf( url.getPort() ), url.getProtocol().equals( CatalogClient.HTTPS ) );
    catalogClient.getAuthentication().login( username, password );

    return catalogClient.getDataResources().read( id );
  }

  public static FacetsResult getFacets( Supplier<ConnectionManager> connManagerSupplier, CCombo wConnection, TransMeta transMeta ) {

    CatalogDetails catalogDetails = (CatalogDetails) connManagerSupplier.get().getConnectionDetails( CatalogDetails.CATALOG, wConnection.getText() );
    FacetsResult result = new FacetsResult();

    try {
      URL url = new URL( transMeta.environmentSubstitute( catalogDetails.getUrl() ) );
      String username = transMeta.environmentSubstitute( catalogDetails.getUsername() );
      String password = transMeta.environmentSubstitute( catalogDetails.getPassword() );

      CatalogClient catalogClient = new CatalogClient( url.getHost(), String.valueOf( url.getPort() ),
          url.getProtocol().equals( CatalogClient.HTTPS ) );

      catalogClient.getAuthentication().login( username, password );
      result = catalogClient.getSearch().doFacets();
    } catch ( MalformedURLException mue ) {
            // Do nothing.
    }

    return result;
  }

  public static void recursiveSetEnabled( Control ctrl, boolean enabled ) {
    if ( ctrl instanceof Composite ) {
      Composite comp = (Composite) ctrl;
      for ( Control c : comp.getChildren() ) {
        recursiveSetEnabled( c, enabled );
      }
    } else {
      ctrl.setEnabled( enabled );
    }
  }
}
