/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

package org.pentaho.amazon.hive.ui;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.eclipse.swt.widgets.Text;
import org.pentaho.amazon.AbstractAmazonJobEntry;
import org.pentaho.amazon.AbstractAmazonJobExecutorController;
import org.pentaho.amazon.hive.job.AmazonHiveJobExecutor;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.database.dialog.tags.ExtTextbox;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

import java.lang.reflect.InvocationTargetException;

/**
 * AmazonHiveJobExecutorController: Handles the attribute dialog box UI for AmazonHiveJobExecutor class.
 */
public class AmazonHiveJobExecutorController extends AbstractAmazonJobExecutorController {

  private static final Class<?> PKG = AmazonHiveJobExecutor.class;

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
  public void browseQ() throws KettleException, FileSystemException {
    String[] fileFilters = new String[] { "*.*" }; //$NON-NLS-1$
    String[] fileFilterNames = new String[] { "All" }; //$NON-NLS-1$

    FileSystemOptions opts = getFileSystemOptions();
    FileObject selectedFile =
      browse( fileFilters, fileFilterNames, getVariableSpace().environmentSubstitute( qUrl ), opts,
        VfsFileChooserDialog.VFS_DIALOG_OPEN_FILE, true );

    if ( selectedFile != null ) {
      setQUrl( selectedFile.getName().getURI() );
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
