/*! ******************************************************************************
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

package org.pentaho.amazon.hive.ui;

import org.eclipse.swt.widgets.Text;
import org.pentaho.amazon.AbstractAmazonJobEntry;
import org.pentaho.amazon.AbstractAmazonJobExecutorController;
import org.pentaho.amazon.hive.job.AmazonHiveJobExecutor;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.database.dialog.tags.ExtTextbox;
import org.pentaho.di.ui.core.events.dialog.ConnectionFilterType;
import org.pentaho.di.ui.core.events.dialog.FilterType;
import org.pentaho.di.ui.core.events.dialog.ProviderFilterType;
import org.pentaho.di.ui.core.events.dialog.SelectionAdapterFileDialogTextVar;
import org.pentaho.di.ui.core.events.dialog.SelectionAdapterOptions;
import org.pentaho.di.ui.core.events.dialog.SelectionOperation;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingFactory;

import java.lang.reflect.InvocationTargetException;

/**
 * AmazonHiveJobExecutorController: Handles the attribute dialog box UI for AmazonHiveJobExecutor class.
 */
public class AmazonHiveJobExecutorController extends AbstractAmazonJobExecutorController {

  private static final Class<?> PKG = AmazonHiveJobExecutor.class;
  private LogChannel log;

  // Define string names for the attributes.
  public static final String Q_URL = "qUrl";
  public static final String BOOTSTRAP_ACTIONS = "bootstrapActions";

  /* XUL Element id's */
  public static final String XUL_QURL = "q-url";
  public static final String XUL_BOOTSTRAP_ACTIONS = "bootstrap-actions";

  // Attributes
  private String qUrl = "";
  private String bootstrapActions = "";

  private AmazonHiveJobExecutor jobEntry;


  public AmazonHiveJobExecutorController( XulDomContainer container, AbstractAmazonJobEntry jobEntry,
                                          BindingFactory bindingFactory ) {

    super( container, jobEntry, bindingFactory );
    this.jobEntry = (AmazonHiveJobExecutor) jobEntry;
    log = new LogChannel( this.jobEntry );
    initializeEmrSettingsGroupMenuFields();
  }

  @Override
  protected void initializeTextFields() {
    super.initializeTextFields();
    ExtTextbox tempBox = (ExtTextbox) container.getDocumentRoot().getElementById( XUL_QURL );
    tempBox.setVariableSpace( getVariableSpace() );
    tempBox = (ExtTextbox) container.getDocumentRoot().getElementById( XUL_BOOTSTRAP_ACTIONS );
    tempBox.setVariableSpace( getVariableSpace() );
  }

  protected void createBindings() {

    super.createBindings();
    bindingFactory.setBindingType( Binding.Type.BI_DIRECTIONAL );
    bindingFactory.createBinding( XUL_BOOTSTRAP_ACTIONS, "value", this, BOOTSTRAP_ACTIONS );
    bindingFactory.createBinding( XUL_QURL, "value", this, Q_URL );

    initializeTextFields();
  }

  @Override
  public String getDialogElementId() {
    return "amazon-emr-job-entry-dialog";
  }

  @Override
  protected void syncModel() {
    super.syncModel();
    ExtTextbox tempBox =
      (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( XUL_BOOTSTRAP_ACTIONS ); //$NON-NLS-1$
    this.bootstrapActions = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( XUL_QURL ); //$NON-NLS-1$
    this.qUrl = ( (Text) tempBox.getTextControl() ).getText();
  }

  @Override
  protected String buildValidationErrorMessages() {
    String validationErrors = super.buildValidationErrorMessages();
    if ( StringUtil.isEmpty( qUrl ) ) {
      validationErrors += BaseMessages.getString( PKG, "AmazonHiveJobExecutor.QURL.Error" ) + "\n";
    }
    return validationErrors;
  }

  @Override
  protected void configureJobEntry() {
    super.configureJobEntry();
    jobEntry.setQUrl( qUrl );
    jobEntry.setBootstrapActions( bootstrapActions );
  }

  /**
   * Initialize the dialog by loading model data, creating bindings and firing initial sync
   *
   * @throws XulException
   * @throws InvocationTargetException
   */
  public void init() throws XulException, InvocationTargetException {

    createBindings();

    super.init();
    if ( jobEntry != null ) {
      setQUrl( jobEntry.getQUrl() );
      setBootstrapActions( jobEntry.getBootstrapActions() );
    }
  }

  /*
   * Open VFS Browser when the "Browse..." button next to the "Hive Script" text box is pressed in the attribute dialog
   * box.
   */
  public void browseQ() {
    SelectionAdapterOptions options =
      new SelectionAdapterOptions( SelectionOperation.FILE,
        new FilterType[] { FilterType.ALL }, FilterType.ALL,
        new ProviderFilterType[] { ProviderFilterType.LOCAL, ProviderFilterType.VFS } );
    options.getConnectionFilters().add( ConnectionFilterType.S3 );
    TextVar textVar = ( (ExtTextbox) container.getDocumentRoot().getElementById( XUL_QURL ) ).extText;
    SelectionAdapterFileDialogTextVar selectionAdapterFileDialogTextVar =
      new SelectionAdapterFileDialogTextVar( log, textVar, getJobEntry().getParentJobMeta(), options );

    selectionAdapterFileDialogTextVar.widgetSelected( null );

    String file = textVar.getText();
    if ( file != null ) {
      setQUrl( file );
    }
  }

  public String getQUrl() {
    return qUrl;
  }

  public void setQUrl( String qUrl ) {
    String previousVal = this.qUrl;
    String newVal = qUrl;

    this.qUrl = qUrl;
    firePropertyChange( AmazonHiveJobExecutorController.Q_URL, previousVal, newVal );
  }

  public String getBootstrapActions() {
    return bootstrapActions;
  }

  public void setBootstrapActions( String bootstrapActions ) {
    String previousVal = this.bootstrapActions;
    String newVal = bootstrapActions;

    this.bootstrapActions = bootstrapActions;
    firePropertyChange( BOOTSTRAP_ACTIONS, previousVal, newVal );
  }

  public void invertAlive() {
    setAlive( !isAlive() );
  }

  public void invertBlocking() {
    setBlocking( !getBlocking() );
  }

  @Override
  public AbstractAmazonJobEntry getJobEntry() {
    return this.jobEntry;
  }

  @Override
  public void setJobEntry( AbstractAmazonJobEntry jobEntry ) {
    this.jobEntry = (AmazonHiveJobExecutor) jobEntry;
  }
}
