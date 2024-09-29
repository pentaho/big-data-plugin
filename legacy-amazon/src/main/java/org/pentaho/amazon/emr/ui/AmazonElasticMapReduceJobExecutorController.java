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


package org.pentaho.amazon.emr.ui;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.eclipse.swt.widgets.Text;
import org.pentaho.amazon.AbstractAmazonJobEntry;
import org.pentaho.amazon.AbstractAmazonJobExecutorController;
import org.pentaho.amazon.emr.job.AmazonElasticMapReduceJobExecutor;
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

public class AmazonElasticMapReduceJobExecutorController extends AbstractAmazonJobExecutorController {

  private static final Class<?> PKG = AmazonElasticMapReduceJobExecutor.class;

  // Define string names for the attributes.
  public static final String JAR_URL = "jarUrl";

  public static final String XUL_JAR_URL = "jar-url";

  private AmazonElasticMapReduceJobExecutor jobEntry;
  private String jarUrl = "";

  public AmazonElasticMapReduceJobExecutorController( XulDomContainer container, AbstractAmazonJobEntry jobEntry,
                                                      BindingFactory bindingFactory ) {

    super( container, jobEntry, bindingFactory );
    this.jobEntry = (AmazonElasticMapReduceJobExecutor) jobEntry;
    initializeEmrSettingsGroupMenuFields();
  }

  @Override
  protected void initializeTextFields() {
    super.initializeTextFields();
    ExtTextbox tempBox = (ExtTextbox) container.getDocumentRoot().getElementById( XUL_JAR_URL );
    tempBox.setVariableSpace( getVariableSpace() );
  }

  protected void createBindings() {

    super.createBindings();
    bindingFactory.setBindingType( Binding.Type.BI_DIRECTIONAL );
    bindingFactory.createBinding( XUL_JAR_URL, "value", this, JAR_URL );
    initializeTextFields();
  }

  @Override
  public String getDialogElementId() {
    return "amazon-emr-job-entry-dialog";
  }

  @Override
  protected void syncModel() {
    super.syncModel();
    ExtTextbox tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( XUL_JAR_URL );
    this.jarUrl = ( (Text) tempBox.getTextControl() ).getText();
  }

  public String getJarUrl() {
    return jarUrl;
  }

  public void setJarUrl( String jarUrl ) {
    String previousVal = this.jarUrl;
    String newVal = jarUrl;

    this.jarUrl = jarUrl;
    firePropertyChange( AmazonElasticMapReduceJobExecutorController.JAR_URL, previousVal, newVal );

  }

  @Override
  protected void configureJobEntry() {
    super.configureJobEntry();
    jobEntry.setJarUrl( jarUrl );
  }

  @Override
  protected String buildValidationErrorMessages() {
    String validationErrors = super.buildValidationErrorMessages();
    if ( StringUtil.isEmpty( jarUrl ) ) {
      validationErrors +=
        BaseMessages.getString( PKG, "AmazonElasticMapReduceJobExecutor.JarURL.Error" ) + "\n";
    }
    return validationErrors;
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
      setJarUrl( jobEntry.getJarUrl() );
    }
  }

  public void browseJar() throws KettleException, FileSystemException {
    String[] fileFilters = new String[] { "*.jar;*.zip" };
    String[] fileFilterNames = new String[] { "Java Archives (jar)" };

    FileSystemOptions opts = getFileSystemOptions();

    FileObject selectedFile =
      browse( fileFilters, fileFilterNames, getVariableSpace().environmentSubstitute( jarUrl ), opts,
      VfsFileChooserDialog.VFS_DIALOG_OPEN_FILE, true );

    if ( selectedFile != null ) {
      setJarUrl( selectedFile.getName().getURI() );
    }
  }

  @Override
  public AbstractAmazonJobEntry getJobEntry() {
    return this.jobEntry;
  }

  @Override
  public void setJobEntry( AbstractAmazonJobEntry jobEntry ) {
    this.jobEntry = (AmazonElasticMapReduceJobExecutor) jobEntry;
  }
}
