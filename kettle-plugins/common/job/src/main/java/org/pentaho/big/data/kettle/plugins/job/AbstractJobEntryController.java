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


package org.pentaho.big.data.kettle.plugins.job;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.big.data.plugins.common.ui.VfsFileChooserHelper;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.plugins.JobEntryPluginType;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.util.HelpUtils;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.containers.XulDeck;
import org.pentaho.ui.xul.containers.XulDialog;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;
import org.pentaho.ui.xul.stereotype.Bindable;
import org.pentaho.ui.xul.swt.tags.SwtDialog;
import com.google.common.annotations.VisibleForTesting;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * User: RFellows Date: 6/6/12
 */
public abstract class AbstractJobEntryController<C extends BlockableJobConfig, E extends AbstractJobEntry<C>> extends
    AbstractXulEventHandler {

  public static final String[] DEFAULT_FILE_FILTERS = new String[] { "*.*" };

  // Generically typed fields
  protected C config; // BlockableJobConfig
  protected E jobEntry; // AbstractJobEntry<BlockableJobConfig>

  // common fields
  protected XulDomContainer container;
  protected BindingFactory bindingFactory;
  protected List<Binding> bindings;
  protected JobMeta jobMeta;

  protected JobEntryMode jobEntryMode = JobEntryMode.QUICK_SETUP;

  @SuppressWarnings( "unchecked" )
  public AbstractJobEntryController( JobMeta jobMeta, XulDomContainer container, E jobEntry,
                                     BindingFactory bindingFactory ) {
    super();
    this.jobMeta = jobMeta;
    this.jobEntry = jobEntry;
    this.container = container;
    this.config = (C) jobEntry.getJobConfig().clone();
    this.bindingFactory = bindingFactory;
  }

  /**
   * @return the simple name for this controller. This controller can be referenced by this name in the XUL document.
   */
  @Override
  public String getName() {
    return "controller";
  }

  /**
   * Opens the dialog
   * 
   * @return
   */
  public JobEntryInterface open() {
    XulDialog dialog = (XulDialog) container.getDocumentRoot().getElementById( getDialogElementId() );
    // Update the Variable Space so the latest are available when the dialog is show (for test evaluation)
    jobEntry.copyVariablesFrom( jobMeta );
    dialog.show();
    return jobEntry;
  }

  /**
   * Initialize the dialog by loading model data, creating bindings and firing initial sync (
   * {@link Binding#fireSourceChanged()}.
   *
   * @throws XulException
   *
   * @throws InvocationTargetException
   *
   */
  public void init() throws XulException, InvocationTargetException {
    bindings = new ArrayList<Binding>();

    // override hook
    beforeInit();

    try {

      createBindings( config, container, bindingFactory, bindings );
      syncModel();

      for ( Binding binding : bindings ) {
        binding.fireSourceChanged();
      }
    } finally {
      // override hook
      afterInit();
    }

  }

  /**
   * Accept and apply the changes made in the dialog. Also, close the dialog
   */
  @Bindable
  public void accept() {
    jobEntry.setJobConfig( config );
    jobEntry.setChanged();
    cancel();
  }

  /**
   * Close the dialog without saving any changes
   */
  @Bindable
  public void cancel() {
    removeBindings();
    XulDialog xulDialog = getDialog();

    Shell shell = (Shell) xulDialog.getRootObject();
    if ( !shell.isDisposed() ) {
      WindowProperty winprop = new WindowProperty( shell );
      PropsUI.getInstance().setScreen( winprop );
      ( (Composite) xulDialog.getManagedObject() ).dispose();
      shell.dispose();
    }
  }

  /**
   * Call help dialog
   */
  public void help() {
    XulDialog xulDialog = getDialog();
    Shell shell = (Shell) xulDialog.getRootObject();
    HelpUtils.openHelpDialog( shell, getPlugin() );
  }

  /**
   * Find a plugin for a corresponding job entry
   */
  protected PluginInterface getPlugin() {
    return PluginRegistry.getInstance().findPluginWithName( JobEntryPluginType.class, jobEntry.getName() );
  }

  /**
   * Remove and destroy all bindings from {@link #bindings}.
   */
  protected void removeBindings() {
    if ( bindings == null ) {
      return;
    }
    for ( Binding binding : bindings ) {
      binding.destroyBindings();
    }
    bindings.clear();
  }

  /**
   * Look up the dialog reference from the document.
   *
   * @return The dialog element referred to by {@link #getDialogElementId()}
   */
  protected SwtDialog getDialog() {
    return (SwtDialog) getXulDomContainer().getDocumentRoot().getElementById( getDialogElementId() );
  }

  /**
   * @return the job entry this controller will modify configuration for
   */
  @VisibleForTesting
  public E getJobEntry() {
    return jobEntry;
  }

  /**
   * Override this to execute some code prior to the init function running
   */
  protected void beforeInit() {
    return;
  }

  /**
   * Override this to execute some code after the init function is complete
   */
  protected void afterInit() {
    return;
  }

  protected boolean showConfirmationDialog( String title, String message ) {
    return MessageDialog.openConfirm( getShell(), title, message );
  }

  /**
   * Show an information dialog with the title and message provided.
   *
   * @param title
   *          Dialog window title
   * @param message
   *          Dialog message
   */
  protected void showInfoDialog( String title, String message ) {
    MessageBox mb = new MessageBox( getShell(), SWT.OK | SWT.ICON_INFORMATION );
    mb.setText( title );
    mb.setMessage( message );
    mb.open();
  }

  /**
   * Show an error dialog with the title and message provided.
   *
   * @param title
   *          Dialog window title
   * @param message
   *          Dialog message
   */
  protected void showErrorDialog( String title, String message ) {
    MessageBox mb = new MessageBox( getShell(), SWT.OK | SWT.ICON_ERROR );
    mb.setText( title );
    mb.setMessage( message );
    mb.open();
  }

  /**
   * Show an error dialog with the title, message, and toggle button to see the entire stacktrace produced by {@code t}.
   *
   * @param title
   *          Dialog window title
   * @param message
   *          Dialog message
   * @param t
   *          Cause for this error
   */
  protected void showErrorDialog( String title, String message, Throwable t ) {
    new ErrorDialog( getShell(), title, message, t );
  }

  /**
   * @return the shell for the currently visible dialog. This will be used to display additional dialogs/popups.
   */
  protected Shell getShell() {
    return getDialog().getShell();
  }

  /**
   * Browse for a file or directory with the VFS Browser.
   *
   * @param root
   *          Root object
   * @param initial
   *          Initial file or folder the browser should open to
   * @param dialogMode
   *          Mode to open dialog in: e.g.
   *          {@link org.pentaho.vfs.ui .VfsFileChooserDialog#VFS_DIALOG_OPEN_FILE_OR_DIRECTORY}
   * @param schemeRestriction
   *          Scheme to limit the user to browsing from
   * @param defaultScheme
   *          Scheme to select by default in the selection dropdown
   * @return The selected file object, {@code null} if no object is selected
   * @throws KettleFileException
   *           Error accessing the root file using the initial file, when {@code root} is not provided
   */
  protected FileObject browseVfs( FileObject root, FileObject initial, int dialogMode, String schemeRestriction,
      String defaultScheme, boolean showFileScheme ) throws KettleFileException {
    String[] schemeRestrictions = new String[1];
    schemeRestrictions[0] = schemeRestriction;
    return browseVfs( root, initial, dialogMode, schemeRestrictions, showFileScheme, defaultScheme );
  }

  protected FileObject browseVfs( FileObject root, FileObject initial, int dialogMode, String[] schemeRestrictions,
      boolean showFileScheme, String defaultScheme ) throws KettleFileException {
    return browseVfs( root, initial, dialogMode, schemeRestrictions, showFileScheme, defaultScheme, null );
  }

  protected FileObject browseVfs( FileObject root, FileObject initial, int dialogMode, String[] schemeRestrictions,
      boolean showFileScheme, String defaultScheme, NamedCluster namedCluster ) throws KettleFileException {
    return browseVfs( root, initial, dialogMode, schemeRestrictions,  showFileScheme, defaultScheme, namedCluster, true, true );
  }

  protected FileObject browseVfs( FileObject root, FileObject initial, int dialogMode, String[] schemeRestrictions,
      boolean showFileScheme, String defaultScheme, NamedCluster namedCluster, boolean showLocation ) throws KettleFileException {
    return browseVfs( root, initial, dialogMode, schemeRestrictions,  showFileScheme, defaultScheme, namedCluster, showLocation, true );
  }

  protected FileObject browseVfs( FileObject root, FileObject initial, int dialogMode, String[] schemeRestrictions,
      boolean showFileScheme, String defaultScheme, NamedCluster namedCluster, boolean showLocation, boolean showCustomUI ) throws KettleFileException {
    Spoon spoon = Spoon.getInstance();
    if ( initial == null ) {
      initial = KettleVFS.getInstance( spoon.getExecutionBowl() )
        .getFileObject( Spoon.getInstance().getLastFileOpened() );
    }
    if ( root == null ) {
      try {
        root = initial.getFileSystem().getRoot();
      } catch ( FileSystemException e ) {
        throw new KettleFileException( e );
      }
    }

    VfsFileChooserHelper fileChooserHelper =
        new VfsFileChooserHelper( getShell(), Spoon.getInstance().getVfsFileChooserDialog( root, initial ), jobEntry );
    fileChooserHelper.setDefaultScheme( defaultScheme );
    fileChooserHelper.setSchemeRestrictions( schemeRestrictions );
    fileChooserHelper.setShowFileScheme( showFileScheme );
    if ( namedCluster != null ) {
      fileChooserHelper.setNamedCluster( namedCluster );
    }
    try {
      return fileChooserHelper.browse(
          getFileFilters(), getFileFilterNames(), initial.getName().getURI(), dialogMode, showLocation, showCustomUI );
    } catch ( KettleException e ) {
      throw new KettleFileException( e );
    } catch ( FileSystemException e ) {
      throw new KettleFileException( e );
    }
  }

  protected String[] getFileFilters() {
    return DEFAULT_FILE_FILTERS;
  }

  /**
   * Used by browseVfs method as names corresponding to the file filters. Override if {@code getFileFilters} is
   * overridden.
   * 
   * @return
   */
  protected String[] getFileFilterNames() {
    return new String[] { BaseMessages.getString( getClass(), "System.FileType.AllFiles" ) };
  }

  /**
   * @return the current configuration object. This configuration may be discarded if the dialog is canceled.
   */
  public C getConfig() {
    return config;
  }

  public void setConfig( C config ) {
    this.config = config;
  }

  /**
   * @return the job meta for the job entry we're editing
   */
  public JobMeta getJobMeta() {
    return jobMeta;
  }

  public void setJobMeta( JobMeta jobMeta ) {
    this.jobMeta = jobMeta;
  }

  public JobEntryMode getJobEntryMode() {
    return jobEntryMode;
  }

  /**
   * Toggle between Advanced and Basic configuration modes
   */
  public void toggleMode() {
    JobEntryMode mode =
        ( jobEntryMode == JobEntryMode.ADVANCED_LIST ? JobEntryMode.QUICK_SETUP : JobEntryMode.ADVANCED_LIST );
    setMode( mode );
  }

  protected void setMode( JobEntryMode mode ) {

    // if switching from Advanced to Quick mode, warn the user that any changes made in Advanced mode will be lost
    if ( this.jobEntryMode == JobEntryMode.ADVANCED_LIST && mode == JobEntryMode.QUICK_SETUP ) {
      boolean confirmed =
          showConfirmationDialog( BaseMessages.getString( AbstractJobEntryController.class,
              "JobExecutor.Confirm.Toggle.Quick.Mode.Title" ), BaseMessages.getString( AbstractJobEntryController.class,
              "JobExecutor.Confirm.Toggle.Quick.Mode.Message" ) );
      if ( !confirmed ) {
        return;
      }
    }

    JobEntryMode opposite = mode == JobEntryMode.QUICK_SETUP ? JobEntryMode.ADVANCED_LIST : JobEntryMode.QUICK_SETUP;
    this.jobEntryMode = mode;
    XulDeck deck = (XulDeck) getXulDomContainer().getDocumentRoot().getElementById( getModeDeckElementId() );

    deck.setSelectedIndex( mode == JobEntryMode.QUICK_SETUP ? 0 : 1 );
    // Synchronize the model every time we swap modes so the UI is always up to date. This is required since we don't
    // set argument item values directly or listen for their changes
    syncModel();

    // Swap the label on the button
    setModeToggleLabel( opposite );
  }

  /**
   * The mode deck element defined in your xul. Override this to customize the element id
   * 
   * @return
   */
  protected String getModeDeckElementId() {
    return "modeDeck";
  }

  // //////////////////
  // abstract methods
  // //////////////////
  protected abstract void syncModel();

  protected abstract void createBindings( C config, XulDomContainer container, BindingFactory bindingFactory,
      Collection<Binding> bindings );

  protected abstract String getDialogElementId();

  protected abstract void setModeToggleLabel( JobEntryMode mode );

}
