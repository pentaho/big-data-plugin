/**
 * ****************************************************************************
 * <p/>
 * Pentaho Big Data
 * <p/>
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 * <p/>
 * ******************************************************************************
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * <p/>
 * ****************************************************************************
 */
package org.pentaho.di.ui.core.config.dialog;

import org.eclipse.swt.SWT;
import org.pentaho.di.core.gui.SpoonFactory;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.spoon.ISpoonMenuController;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.SpoonLifecycleListener;
import org.pentaho.di.ui.spoon.SpoonPerspective;
import org.pentaho.di.ui.spoon.SpoonPlugin;
import org.pentaho.di.ui.spoon.SpoonPluginCategories;
import org.pentaho.di.ui.spoon.SpoonPluginInterface;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.dom.Document;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;

import java.util.Enumeration;
import java.util.ResourceBundle;

/**
 * Spoon plugin to add "Hadoop Configuration" menu to the View drop-down menu. This allows the Spoon user to display
 * the active Hadoop configuration properties
 */
@SpoonPlugin( id = "ActiveHadoopConfigPropertiesSpoonPlugin", image = "" )
@SpoonPluginCategories( { "spoon" } )
public class ActiveHadoopConfigPropertiesSpoonPlugin extends AbstractXulEventHandler implements SpoonPluginInterface, ISpoonMenuController, SpoonLifecycleListener {

  private static Class<?> PKG = ActiveHadoopConfigPropertiesDialog.class; // for i18n purposes, needed by Translator2!!

  ResourceBundle bundle = new ResourceBundle() {
    @Override
    public Enumeration<String> getKeys() {
      return null;
    }

    @Override
    protected Object handleGetObject( String key ) {
      return BaseMessages.getString( ActiveHadoopConfigPropertiesSpoonPlugin.class, key );
    }
  };

  public String getName() {
    return "activeHadoopConfig"; //$NON-NLS-1$
  }

  @Override
  public void onEvent( SpoonLifeCycleEvent evt ) {
    if ( evt.equals( SpoonLifeCycleEvent.STARTUP ) ) {
      Spoon spoon = ( (Spoon) SpoonFactory.getInstance() );
      spoon.addSpoonMenuController( this );
    }
  }

  @Override
  public void applyToContainer( String category, XulDomContainer container ) throws XulException {
    ClassLoader cl = getClass().getClassLoader();
    container.registerClassLoader( cl );
    if ( category.equals( "spoon" ) ) {
      container.loadOverlay( "org/pentaho/di/ui/core/config/dialog/menubar_overlay.xul", bundle );
      container.addEventHandler( this );
    }
  }

  @Override
  public SpoonLifecycleListener getLifecycleListener() {
    return this;
  }

  @Override
  public SpoonPerspective getPerspective() {
    return null;
  }

  @Override
  public void updateMenu( Document doc ) {
    // Empty method
  }

  public void showDialog() {
    final Spoon spoon = Spoon.getInstance();
    try {
      ActiveHadoopConfigPropertiesDialog gcm = new ActiveHadoopConfigPropertiesDialog( spoon.getShell(), SWT.NONE );
      gcm.open();
    } catch ( Exception e ) {
      showErrorDialog( e,
        BaseMessages.getString( PKG, "ActiveHadoopConfigPropertiesDialog.Exception.ErrorLoadingData.Title" ),
        BaseMessages.getString( PKG, "ActiveHadoopConfigPropertiesDialog.Exception.ErrorLoadingData.Message" ) );
    }
  }

  public void init() {

  }

  /**
   * Show an error dialog
   *
   * @param e       The exception to display
   * @param title   The dialog title
   * @param message The message to display
   */
  private void showErrorDialog( Exception e, String title, String message ) {
    new ErrorDialog( Spoon.getInstance().getShell(), title, message, e );
  }
}
