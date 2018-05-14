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

package org.pentaho.big.data.kettle.plugins.sqoop.ui;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.big.data.kettle.plugins.hdfs.vfs.HadoopVfsFileChooserDialog;
import org.pentaho.big.data.kettle.plugins.hdfs.vfs.Schemes;
import org.pentaho.big.data.kettle.plugins.job.AbstractJobEntryController;
import org.pentaho.big.data.kettle.plugins.job.BlockableJobConfig;
import org.pentaho.big.data.kettle.plugins.job.JobEntryMode;
import org.pentaho.big.data.kettle.plugins.job.PropertyEntry;
import org.pentaho.big.data.kettle.plugins.sqoop.AbstractSqoopJobEntry;
import org.pentaho.big.data.kettle.plugins.sqoop.ArgumentWrapper;
import org.pentaho.big.data.kettle.plugins.sqoop.DatabaseItem;
import org.pentaho.big.data.kettle.plugins.sqoop.SqoopConfig;
import org.pentaho.big.data.kettle.plugins.sqoop.SqoopUtils;
import org.pentaho.big.data.plugins.common.ui.HadoopClusterDelegateImpl;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.ui.core.database.dialog.DatabaseDialog;
import org.pentaho.di.ui.core.database.dialog.DatabaseExplorerDialog;
import org.pentaho.di.ui.core.dialog.EnterSelectionDialog;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.ui.xul.XulComponent;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingConvertor;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.components.XulButton;
import org.pentaho.ui.xul.components.XulMenuList;
import org.pentaho.ui.xul.containers.XulDeck;
import org.pentaho.ui.xul.containers.XulDialog;
import org.pentaho.ui.xul.containers.XulTree;
import org.pentaho.ui.xul.util.AbstractModelList;
import org.pentaho.vfs.ui.CustomVfsUiPanel;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

import java.util.Collection;
import java.util.List;

/**
 * Base functionality to support a Sqoop job entry controller that provides most of the common functionality to back a
 * XUL-based dialog.
 *
 * @param <S>
 *          Type of Sqoop configuration object this controller depends upon. Must match the configuration object the job
 *          entry expects.
 */
public abstract class AbstractSqoopJobEntryController<S extends SqoopConfig, E extends AbstractSqoopJobEntry<S>>
    extends AbstractJobEntryController<S, E> {
  public static final String SELECTED_DATABASE_CONNECTION = "selectedDatabaseConnection";
  public static final String MODE_TOGGLE_LABEL = "modeToggleLabel";
  protected static Class<?> PKG = AbstractSqoopJobEntry.class;
  private final String[] MODE_I18N_STRINGS = new String[] { "Sqoop.JobEntry.AdvancedOptions.Button.Text",
    "Sqoop.JobEntry.QuickSetup.Button.Text" };
  public static final String VALUE = "value";

  // This is overwritten in init() with the i18n string
  protected DatabaseItem NO_DATABASE = new DatabaseItem( "@@none@@", "Choose Available" );

  // The following is overwritten in init() with the i18n string
  protected DatabaseItem USE_ADVANCED_OPTIONS = new DatabaseItem( "@@advanced@@", "Use Advanced Options" );

  private NamedCluster CHOOSE_AVAILABLE_CLUSTER;
  private NamedCluster USE_ADVANCED_OPTIONS_CLUSTER;

  protected AbstractModelList<ArgumentWrapper> advancedArguments;
  private AbstractModelList<DatabaseItem> databaseConnections;
  private AbstractModelList<NamedCluster> namedClusters;
  private DatabaseItem selectedDatabaseConnection;
  private DatabaseDialog databaseDialog;
  protected NamedCluster selectedNamedCluster;

  // Flag to indicate we shouldn't handle any events. Useful for preventing unwanted synchronization during
  // initialization
  // or other user-driven events.
  protected boolean suppressEventHandling = false;

  protected final HadoopClusterDelegateImpl ncDelegate;

  /**
   * The text for the Quick Setup/Advanced Options mode toggle (label).
   */
  private String modeToggleLabel;
  private AdvancedButton selectedAdvancedButton = AdvancedButton.LIST;

  protected enum AdvancedButton {
    LIST( 0, JobEntryMode.ADVANCED_LIST ),
    COMMAND_LINE( 1, JobEntryMode.ADVANCED_COMMAND_LINE );

    private int deckIndex;
    private JobEntryMode mode;

    AdvancedButton( int deckIndex, JobEntryMode mode ) {
      this.deckIndex = deckIndex;
      this.mode = mode;
    }

    public int getDeckIndex() {
      return deckIndex;
    }

    public JobEntryMode getMode() {
      return mode;
    }
  }

  /**
   * Creates a new Sqoop job entry controller.
   *
   * @param jobMeta
   *          Meta data for the job
   * @param container
   *          Container with dialog for which we will control
   * @param jobEntry
   *          Job entry the dialog is being created for
   * @param bindingFactory
   *          Binding factory to generate bindings
   */
  @SuppressWarnings( "unchecked" )
  public AbstractSqoopJobEntryController( JobMeta jobMeta, XulDomContainer container, E jobEntry,
      BindingFactory bindingFactory ) {
    super( jobMeta, container, jobEntry, bindingFactory );
    this.config = (S) jobEntry.getJobConfig().clone();

    NamedClusterService namedClusterService = jobEntry.getNamedClusterService();
    this.advancedArguments = new AbstractModelList<ArgumentWrapper>();
    this.databaseConnections = new AbstractModelList<DatabaseItem>();
    this.namedClusters = new AbstractModelList<NamedCluster>();
    ncDelegate = new HadoopClusterDelegateImpl( Spoon.getInstance(), namedClusterService,
      jobEntry.getRuntimeTestActionService(), jobEntry.getRuntimeTester() );

    CHOOSE_AVAILABLE_CLUSTER = namedClusterService.getClusterTemplate();
    CHOOSE_AVAILABLE_CLUSTER.setName( BaseMessages.getString( AbstractSqoopJobEntry.class,
        "DatabaseName.ChooseAvailable" ) );
    CHOOSE_AVAILABLE_CLUSTER.setVariable( "valid", "false" );

    USE_ADVANCED_OPTIONS_CLUSTER = namedClusterService.getClusterTemplate();
    USE_ADVANCED_OPTIONS_CLUSTER.setName( BaseMessages.getString( AbstractSqoopJobEntry.class,
        "DatabaseName.UseAdvancedOptions" ) );
    USE_ADVANCED_OPTIONS_CLUSTER.setVariable( "valid", "false" );
  }

  /**
   * @return the element id of the XUL dialog element in the XUL document
   */
  public abstract String getDialogElementId();

  /**
   * Create the necessary XUL {@link Binding}s to support the dialog's desired functionality.
   *
   * @param config
   *          Configuration object to bind to
   * @param container
   *          Container with components to bind to
   * @param bindingFactory
   *          Binding factory to create bindings with
   * @param bindings
   *          Collection to add created bindings to. This collection will be initialized via
   *          {@link org.pentaho.ui.xul.binding.Binding#fireSourceChanged()} upon return.
   */
  @Override
  protected void createBindings( S config, XulDomContainer container, BindingFactory bindingFactory,
      Collection<Binding> bindings ) {

    bindingFactory.setBindingType( Binding.Type.BI_DIRECTIONAL );
    bindings.add( bindingFactory.createBinding( config, BlockableJobConfig.JOB_ENTRY_NAME, BlockableJobConfig.JOB_ENTRY_NAME, VALUE ) );

    // TODO Determine if separate schema field is required, this has to be provided as part of the --table argument
    // anyway.
    // bindings.add(bindingFactory.createBinding(config, SCHEMA, SCHEMA, VALUE));
    bindings.add( bindingFactory.createBinding( config, SqoopConfig.TABLE, SqoopConfig.TABLE, VALUE ) );

    bindings.add( bindingFactory.createBinding( config, SqoopConfig.COMMAND_LINE, SqoopConfig.COMMAND_LINE, VALUE ) );

    bindingFactory.setBindingType( Binding.Type.ONE_WAY );
    bindings.add( bindingFactory.createBinding( this, "modeToggleLabel", getModeToggleLabelElementId(), VALUE ) );
    bindings.add( bindingFactory.createBinding( databaseConnections, "children", "connection", "elements" ) );
    bindings.add( bindingFactory.createBinding( namedClusters, "children", "named-clusters", "elements" ) );

    XulTree variablesTree = (XulTree) container.getDocumentRoot().getElementById( "advanced-table" );
    bindings.add( bindingFactory.createBinding( advancedArguments, "children", variablesTree, "elements" ) );

    XulTree customVariablesTree = (XulTree) container.getDocumentRoot().getElementById( "custom-table" );
    bindings.add( bindingFactory.createBinding( config.getCustomArguments(), "children", customVariablesTree, "elements" ) );

    // Create database/connection sync so that we're notified any time the connect argument is updated
    bindingFactory.createBinding( config, "connect", this, "connectChanged" );
    bindingFactory.createBinding( config, "username", this, "usernameChanged" );
    bindingFactory.createBinding( config, "password", this, "passwordChanged" );

    bindingFactory.createBinding( config, SqoopConfig.NAMENODE_HOST, this, "advancedNamedConfiguration" );
    bindingFactory.createBinding( config, SqoopConfig.NAMENODE_PORT, this, "advancedNamedConfiguration" );
    bindingFactory.createBinding( config, SqoopConfig.JOBTRACKER_HOST, this, "advancedNamedConfiguration" );
    bindingFactory.createBinding( config, SqoopConfig.JOBTRACKER_PORT, this, "advancedNamedConfiguration" );

    bindingFactory.setBindingType( Binding.Type.BI_DIRECTIONAL );
    // Specifically create this binding after the databaseConnections binding so the list is populated before we attempt
    // to select an item
    bindings.add( bindingFactory.createBinding( this, SELECTED_DATABASE_CONNECTION, "connection", "selectedItem" ) );
    bindings.add( bindingFactory.createBinding( "named-clusters", "selectedIndex", this, "selectedNamedCluster",
        new BindingConvertor<Integer, NamedCluster>() {
          public NamedCluster sourceToTarget( final Integer index ) {
            if ( index == -1 ) {
              return null;
            }
            return namedClusters.get( index );
          }

          public Integer targetToSource( final NamedCluster namedCluster ) {
            return namedClusters.indexOf( namedCluster );
          }
        } ) );
  }

  protected void initializeNamedClusterSelection() {
    @SuppressWarnings( "unchecked" )
    XulMenuList<NamedCluster> namedClusterMenu =
        (XulMenuList<NamedCluster>) container.getDocumentRoot().getElementById( "named-clusters" ); //$NON-NLS-1$
    try {
      String cn = config.getClusterName();
      if ( cn != null ) {
        NamedCluster namedCluster = jobEntry.getNamedClusterService().read( cn, getMetaStore() );
        namedClusterMenu.setSelectedItem( namedCluster );
        setSelectedNamedCluster( namedCluster );
      } else if ( config.isAdvancedClusterConfigSet() ) {
        setSelectedNamedCluster( USE_ADVANCED_OPTIONS_CLUSTER );
      } else {
        setSelectedNamedCluster( CHOOSE_AVAILABLE_CLUSTER );
      }
    } catch ( MetaStoreException e ) {
      jobEntry.logError( e.getMessage() );
    }
  }

  public AbstractModelList<NamedCluster> getNamedClusters() {
    return this.namedClusters;
  }

  public void setNamedClusters( AbstractModelList<NamedCluster> namedClusters ) {
    this.namedClusters = namedClusters;
  }

  public void setSelectedNamedCluster( NamedCluster namedCluster ) {
    this.selectedNamedCluster = namedCluster;
    if ( !suppressEventHandling ) {
      if ( CHOOSE_AVAILABLE_CLUSTER.equals( namedCluster ) ) {
        config.setNamedCluster( null );
      } else if ( USE_ADVANCED_OPTIONS_CLUSTER.equals( namedCluster ) || namedCluster == null ) {
        config.setClusterName( null );
      } else {
        config.setNamedCluster( namedCluster );
      }

      suppressEventHandling = true;
      try {
        firePropertyChange( "selectedNamedCluster", null, this.selectedNamedCluster );
      } finally {
        suppressEventHandling = false;
      }
    }
  }

  private void updateDeleteButton() {
    boolean disabled = config.getCustomArguments().size() == 0;
    XulComponent delete = getXulDomContainer().getDocumentRoot().getElementById( "delete-button" );
    delete.setDisabled( disabled );
  }

  public boolean isSelectedNamedCluster() {
    return this.selectedNamedCluster != null;
  }

  /**
   * @return element id for the deck of dialog modes (quick setup, advanced)
   */
  protected String getModeDeckElementId() {
    return "modeDeck";
  }

  @Override
  protected void beforeInit() {
    NO_DATABASE =
        new DatabaseItem( "@@none@@", BaseMessages.getString( AbstractSqoopJobEntry.class,
            "DatabaseName.ChooseAvailable" ) );
    USE_ADVANCED_OPTIONS =
        new DatabaseItem( "@@advanced@@", BaseMessages.getString( AbstractSqoopJobEntry.class,
            "DatabaseName.UseAdvancedOptions" ) );
    // Suppress event handling while we're initializing to prevent unwanted value changes
    suppressEventHandling = true;
    populateDatabases();
    populateNamedClusters();
    setModeToggleLabel( BaseMessages.getString( AbstractSqoopJobEntry.class, MODE_I18N_STRINGS[0] ) );
    // customizeModeToggleLabel(getModeToggleLabelElementId());
  }

  @Override
  protected void afterInit() {
    setUiMode( getConfig().getModeAsEnum() );

    suppressEventHandling = false;

    // Manually set the current database, if it is valid, to sync the UI buttons since we suppressed their event
    // handling while initializing bindings
    setSelectedDatabaseConnection( createDatabaseItem( getConfig().getDatabase() ) );
    initializeNamedClusterSelection();

    updateDeleteButton();
  }

  /**
   * @return element id for the deck of advanced dialog modes (general, list, command line)
   */
  protected String getAdvancedModeDeckElementId() {
    return "advancedModeDeck";
  }

  /**
   * Synchronize the model values from the configuration object to our internal model objects.
   */
  protected void syncModel() {
    advancedArguments.clear();
    advancedArguments.addAll( config.getAdvancedArgumentsList() );
  }

  /**
   * Populate the list of databases from the {@link JobMeta}.
   */
  protected void populateDatabases() {
    databaseConnections.clear();
    for ( DatabaseMeta dbMeta : jobMeta.getDatabases() ) {
      if ( jobEntry.isDatabaseSupported( dbMeta.getDatabaseInterface().getClass() ) ) {
        databaseConnections.add( new DatabaseItem( dbMeta.getName() ) );
      }
    }
    updateDatabaseItemsList();
  }

  protected void populateNamedClusters() {
    namedClusters.clear();
    try {
      namedClusters.addAll( jobEntry.getNamedClusterService().list( getMetaStore() ) );
    } catch ( MetaStoreException e ) {
      jobEntry.logError( e.getMessage(), e );
    }
    namedClusters.add( CHOOSE_AVAILABLE_CLUSTER );
    namedClusters.add( USE_ADVANCED_OPTIONS_CLUSTER );
  }

  /**
   * This is used to be notified when the connect string changes so we can remove the selection from the database
   * dropdown.
   */
  public void setConnectChanged( String connect ) {
    // If the connect string changes unselect the database
    if ( !suppressEventHandling ) {
      if ( connect != null ) {
        config.copyConnectionInfoToAdvanced();
        setSelectedDatabaseConnection( USE_ADVANCED_OPTIONS );
      } else {
        setSelectedDatabaseConnection( NO_DATABASE );
      }
    }
  }


  public void setAdvancedNamedConfiguration( String value ) {
    if ( !suppressEventHandling ) {
      if ( value != null ) {
        setSelectedNamedCluster( USE_ADVANCED_OPTIONS_CLUSTER );
      }
    }
  }

  public void setUsernameChanged( String username ) {
    if ( !suppressEventHandling ) {
      config.copyConnectionInfoToAdvanced();
      setSelectedDatabaseConnection( USE_ADVANCED_OPTIONS );
    }
  }

  public void setPasswordChanged( String password ) {
    if ( !suppressEventHandling ) {
      config.copyConnectionInfoToAdvanced();
      setSelectedDatabaseConnection( USE_ADVANCED_OPTIONS );
    }
  }

  /**
   * @return the list of database connections that back the connections list
   */
  public AbstractModelList<DatabaseItem> getDatabaseConnections() {
    return databaseConnections;
  }

  /**
   * @return the selected database from the configuration object
   */
  public DatabaseItem getSelectedDatabaseConnection() {
    return selectedDatabaseConnection;
  }

  /**
   * Creates a {@link DatabaseItem} based on the name of a database and the existence of a connection string in the
   * configuration.
   *
   * @param database
   *          Name of database
   * @return A database item whose name is {@code database}. If {@code database} is null, {@link #NO_DATABASE} is
   *         returned iff. {@link SqoopConfig#getConnect() getConfig().getConnect()} is
   *         null; {@link #USE_ADVANCED_OPTIONS} otherwise.
   */
  protected DatabaseItem createDatabaseItem( String database ) {
    return database == null ? ( getConfig().getConnect() != null ? USE_ADVANCED_OPTIONS : NO_DATABASE )
        : new DatabaseItem( database );
  }

  /**
   * Sets the selected database connection. This database will be verified to exist and the appropriate settings within
   * the model will be set.
   *
   * @param selectedDatabaseConnection
   *          Database item to select
   */
  public void setSelectedDatabaseConnection( DatabaseItem selectedDatabaseConnection ) {
    if ( !suppressEventHandling ) {
      this.selectedDatabaseConnection = selectedDatabaseConnection;
      DatabaseMeta databaseMeta =
          this.selectedDatabaseConnection == null ? null : jobMeta.findDatabase( this.selectedDatabaseConnection
              .getName() );
      jobEntry.setUsedDbConnection( databaseMeta );
      boolean validDatabaseSelected = databaseMeta != null;
      setDatabaseInteractionButtonsDisabled( !validDatabaseSelected );
      suppressEventHandling = true;
      try {
        updateDatabaseItemsList();
      } finally {
        suppressEventHandling = false;
      }
      // Always update the database info in case something more than the name changes (we can only tell if database
      // names change via DatabaseItem)
      if ( validDatabaseSelected ) {
        try {
          getConfig().setConnectionInfo( databaseMeta.getName(), databaseMeta.getURL(), databaseMeta.getUsername(),
              databaseMeta.getPassword() );
        } catch ( KettleDatabaseException ex ) {
          jobEntry.logError(
              BaseMessages.getString( AbstractSqoopJobEntry.class, "ErrorConfiguringDatabaseConnection" ), ex );
        }
      } else {
        getConfig().copyConnectionInfoFromAdvanced();
      }
      // Always fire a property change
      suppressEventHandling = true;
      try {
        firePropertyChange( SELECTED_DATABASE_CONNECTION, null, this.selectedDatabaseConnection );
      } finally {
        suppressEventHandling = false;
      }
    }
  }

  /**
   * Make sure we have a "Use Advanced Option" in the list of database connections if we don't have a valid database
   * selected but we have an advanced connect string.
   */
  protected void updateDatabaseItemsList() {
    if ( this.selectedDatabaseConnection == null || NO_DATABASE.equals( this.selectedDatabaseConnection ) ) {
      if ( !databaseConnections.contains( NO_DATABASE ) ) {
        databaseConnections.add( NO_DATABASE );
      }
    } else {
      if ( databaseConnections.contains( NO_DATABASE ) ) {
        databaseConnections.remove( NO_DATABASE );
      }
    }
    if ( getConfig().getConnectFromAdvanced() != null || getConfig().getUsernameFromAdvanced() != null
        || getConfig().getPasswordFromAdvanced() != null ) {
      if ( !databaseConnections.contains( USE_ADVANCED_OPTIONS ) ) {
        databaseConnections.add( USE_ADVANCED_OPTIONS );
      }
    } else {
      if ( databaseConnections.contains( USE_ADVANCED_OPTIONS ) ) {
        databaseConnections.remove( USE_ADVANCED_OPTIONS );
      }
    }
  }

  /**
   * Set the enabled state for all buttons that require a valid database to be selected.
   *
   * @param b
   *          {@code true} if the buttons should be disabled
   */
  protected void setDatabaseInteractionButtonsDisabled( boolean b ) {
    document.getElementById( getEditConnectionButtonId() ).setDisabled( b );
    document.getElementById( getBrowseTableButtonId() ).setDisabled( b );
    // document.getElementById(getBrowseSchemaButtonId()).setDisabled(b);
  }

  /**
   * @return the text for the "modeToggleLabel" label
   */
  public String getModeToggleLabel() {
    return modeToggleLabel;
  }

  /**
   * Set the label text for the mode toggle label element
   *
   * @param modeToggleLabel
   */
  public void setModeToggleLabel( String modeToggleLabel ) {
    String old = this.modeToggleLabel;
    this.modeToggleLabel = modeToggleLabel;
    firePropertyChange( MODE_TOGGLE_LABEL, old, this.modeToggleLabel );
  }

  @Override
  protected void setModeToggleLabel( JobEntryMode mode ) {
    setModeToggleLabel( BaseMessages.getString( AbstractSqoopJobEntry.class, MODE_I18N_STRINGS[mode.ordinal()] ) );
  }

  protected DatabaseDialog getDatabaseDialog() {
    if ( databaseDialog == null ) {
      databaseDialog = new DatabaseDialog( getShell() );
    }
    return databaseDialog;
  }

  public void editConnection() {
    DatabaseMeta current = jobMeta.findDatabase( config.getDatabase() );
    if ( current == null ) {
      return; // nothing to edit, this should not be possible through the UI
    }
    editDatabaseMeta( current, false );
  }

  public void newConnection() {
    editDatabaseMeta( new DatabaseMeta(), true );
  }

  /**
   * Open the Database Connection Dialog to edit
   *
   * @param database
   *          Database meta to edit
   * @param isNew
   *          Is this database meta new? If so and the user chooses to save the database connection we will make sure to
   *          save this into the job meta.
   */
  protected void editDatabaseMeta( DatabaseMeta database, boolean isNew ) {
    database.shareVariablesWith( jobMeta );
    getDatabaseDialog().setDatabaseMeta( database );
    if ( getDatabaseDialog().open() != null ) {
      if ( isNew ) {
        jobMeta.addDatabase( getDatabaseDialog().getDatabaseMeta() );
      }
      suppressEventHandling = true;
      try {
        populateDatabases();
      } finally {
        suppressEventHandling = false;
      }
      setSelectedDatabaseConnection( createDatabaseItem( getDatabaseDialog().getDatabaseMeta().getName() ) );
      jobEntry.setUsedDbConnection( database );
    }
  }

  /**
   * @return the simple name for this controller. This controller can be referenced by this name in the XUL document.
   */
  @Override
  public String getName() {
    return "controller";
  }

  /**
   * @return the edit connection button's id. By default this is {@code "editConnectionButton"}
   */
  public String getEditConnectionButtonId() {
    return "editConnectionButton";
  }

  /**
   * @return the browse table button's id. By default this is {@code "browseTableButton"}
   */
  public String getBrowseTableButtonId() {
    return "browseTableButton";
  }

  public String getBrowseSchemaButtonId() {
    return "browseSchemaButton";
  }

  /**
   * @return the id of the element responsible for toggling between "Quick Setup" and "Advanced Options" modes
   */
  public String getModeToggleLabelElementId() {
    return "mode-toggle-label";
  }

  /**
   * @return the advanced list button's id. By default this is {@code "advanced-list-button"}
   */
  public String getAdvancedListButtonElementId() {
    return "advanced-list-button";
  }

  /**
   * @return the advanced command line button's id. By default this is {@code "advanced-command-line-button"}
   */
  public String getAdvancedCommandLineButtonElementId() {
    return "advanced-command-line-button";
  }

  /**
   * @return the current configuration object. This configuration may be discarded if the dialog is canceled.
   */
  @Override
  public S getConfig() {
    return config;
  }

  /**
   * Test the configuration settings and show a dialog with the feedback.
   */
  public void testSettings() {
    List<String> warnings = jobEntry.getValidationWarnings( getConfig() );
    if ( !warnings.isEmpty() ) {
      StringBuilder sb = new StringBuilder();
      for ( String warning : warnings ) {
        sb.append( warning ).append( "\n" );
      }
      showErrorDialog( BaseMessages.getString( AbstractSqoopJobEntry.class, "ValidationError.Dialog.Title" ), sb
          .toString() );
      return;
    }
    // TODO implement
    showErrorDialog( "Error", "Not Implemented" );
  }

  /**
   * Toggles between Quick Setup and Advanced Options mode. This assumes there exists a deck by id
   * {@link #getModeDeckElementId()} and it contains two panels.
   */
  public void toggleMode() {
    XulDeck deck = getModeDeck();
    setUiMode( deck.getSelectedIndex() == 1 ? JobEntryMode.QUICK_SETUP : selectedAdvancedButton.getMode() );
  }

  /**
   * Toggles between Quick Setup and Advanced Options mode. This assumes there exists a deck by id
   * {@link #getModeDeckElementId()} and it contains two panels.
   *
   * @param quickMode
   *          Should quick mode be visible/selected?
   */
  protected void toggleQuickMode( boolean quickMode ) {
    XulDeck deck = getModeDeck();
    deck.setSelectedIndex( quickMode ? 0 : 1 );

    // Swap the label on the button
    setModeToggleLabel( BaseMessages
        .getString( AbstractSqoopJobEntry.class, MODE_I18N_STRINGS[deck.getSelectedIndex()] ) );

    // We toggle to and from quick setup in this method so either the old or the new is always Mode.QUICK_SETUP.
    // Whichever is not is the mode for the currently selected advanced button
    JobEntryMode oldMode = deck.getSelectedIndex() == 0 ? selectedAdvancedButton.getMode() : JobEntryMode.QUICK_SETUP;
    JobEntryMode newMode =
        JobEntryMode.QUICK_SETUP == oldMode ? selectedAdvancedButton.getMode() : JobEntryMode.QUICK_SETUP;
    updateUiMode( oldMode, newMode );
  }

  protected void setUiMode( JobEntryMode mode ) {
    switch ( mode ) {
      case QUICK_SETUP:
        if ( selectedNamedCluster != null
          && !this.selectedNamedCluster.equals( USE_ADVANCED_OPTIONS_CLUSTER )
          && !suppressEventHandling ) {
          config.setNamedCluster( selectedNamedCluster );
        }
        toggleQuickMode( true );
        break;
      case ADVANCED_LIST:
        setSelectedAdvancedButton( AdvancedButton.LIST );
        toggleQuickMode( false );
        break;
      case ADVANCED_COMMAND_LINE:
        setSelectedAdvancedButton( AdvancedButton.COMMAND_LINE );
        toggleQuickMode( false );
        break;
      default:
        throw new RuntimeException( "unsupported mode: " + mode );
    }
  }

  /**
   * Update the UI Mode and configure the underlying {@link SqoopConfig} object.
   *
   * @param oldMode
   *          Old mode
   * @param newMode
   *          New mode
   */
  protected void updateUiMode( JobEntryMode oldMode, JobEntryMode newMode ) {
    if ( suppressEventHandling ) {
      return;
    }
    if ( JobEntryMode.ADVANCED_COMMAND_LINE.equals( oldMode ) ) {
      if ( !syncCommandLineToConfig() ) {
        // Flip back to the advanced command line view
        // Suppress event handling so we don't re-enter updateUiMode and copy the properties back on top of the command
        // line
        suppressEventHandling = true;
        try {
          setUiMode( JobEntryMode.ADVANCED_COMMAND_LINE );
        } finally {
          suppressEventHandling = false;
        }
        return;
      }
    } else if ( JobEntryMode.ADVANCED_COMMAND_LINE.equals( newMode ) ) {
      // Sync config properties -> command line
      getConfig().setCommandLine( SqoopUtils.generateCommandLineString( getConfig(), null ) );
    }

    if ( JobEntryMode.ADVANCED_LIST.equals( newMode ) ) {
      // Synchronize the model when we switch to the advanced list to make sure it's fresh
      syncModel();
    }

    getConfig().setMode( getMode().name() );
  }

  /**
   * @return the current UI mode based off the current state of the components
   */
  private JobEntryMode getMode() {
    XulDeck modeDeck = getModeDeck();
    XulDeck advancedModeDeck = getAdvancedModeDeck();
    if ( modeDeck.getSelectedIndex() == 0 ) {
      return JobEntryMode.QUICK_SETUP;
    } else {
      for ( AdvancedButton b : AdvancedButton.values() ) {
        if ( b.getDeckIndex() == advancedModeDeck.getSelectedIndex() ) {
          return b.getMode();
        }
      }
    }
    throw new RuntimeException( "unknown UI mode" );
  }

  /**
   * Configure the current config object from the command line string. This will invoke
   * {@link #showErrorDialog(String, String, Throwable)} if an exception occurs.
   *
   * @return {@code true} if the command line could be parsed and the config object updated successfully.
   */
  protected boolean syncCommandLineToConfig() {
    try {
      // Sync command line -> config properties
      SqoopUtils.configureFromCommandLine( getConfig(), getConfig().getCommandLine(), null );
      return true;
    } catch ( Exception ex ) {
      showErrorDialog( BaseMessages.getString( AbstractSqoopJobEntry.class, "Dialog.Error" ), BaseMessages.getString(
          AbstractSqoopJobEntry.class, "ErrorConfiguringFromCommandLine" ), ex );
    }
    return false;
  }

  public void setSelectedAdvancedButton( AdvancedButton button ) {
    AdvancedButton old = selectedAdvancedButton;
    selectedAdvancedButton = button;
    switch ( button ) {
      case LIST:
        XulButton advancedList = getAdvancedListButton();
        advancedList.setSelected( true );
        getAdvancedCommandLineButton().setSelected( false );
        break;
      case COMMAND_LINE:
        getAdvancedListButton().setSelected( false );
        getAdvancedCommandLineButton().setSelected( true );
        break;
      default:
        throw new RuntimeException( "Unknown button type: " + button );
    }
    toggleAdvancedMode( button );
    updateUiMode( old == null ? null : old.getMode(), button.getMode() );
  }

  /**
   * Toggle the selected deck for advanced mode.
   *
   * @param button
   *          Button that was selected
   */
  protected void toggleAdvancedMode( AdvancedButton button ) {
    getAdvancedModeDeck().setSelectedIndex( button.getDeckIndex() );
  }

  /**
   * @return The button to select the advanced list mode
   */
  public XulButton getAdvancedListButton() {
    return getButton( getAdvancedListButtonElementId() );
  }

  /**
   * @return The button to select the advanced command line mode
   */
  public XulButton getAdvancedCommandLineButton() {
    return getButton( getAdvancedCommandLineButtonElementId() );
  }

  /**
   * @return the deck that shows either the Quick Setup or Advanced Mode UI
   */
  protected XulDeck getModeDeck() {
    return (XulDeck) getXulDomContainer().getDocumentRoot().getElementById( getModeDeckElementId() );
  }

  /**
   * @return the deck that contains Advanced Mode panels
   */
  protected XulDeck getAdvancedModeDeck() {
    return (XulDeck) getXulDomContainer().getDocumentRoot().getElementById( getAdvancedModeDeckElementId() );
  }

  /**
   * Gets a {@link XulButton} from the current {@link XulDomContainer}
   *
   * @param elementId
   *          Element Id of the button to look up
   * @return The button with element id {@code elementId} or {@code null} if not found
   */
  protected XulButton getButton( String elementId ) {
    return (XulButton) getXulDomContainer().getDocumentRoot().getElementById( elementId );
  }

  /**
   * Callback for clicking the advanced list button
   */
  public void advancedListButtonClicked() {
    setSelectedAdvancedButton( AdvancedButton.LIST );
  }

  /**
   * Callback for clicking the advanced command line button
   */
  public void advancedCommandLineButtonClicked() {
    setSelectedAdvancedButton( AdvancedButton.COMMAND_LINE );
  }

  /**
   * Show the schema browse dialog if schemas can be detected and exist for the give database. Set the selected schema
   * to {@link SqoopConfig#setSchema(String) getConfig().setSchema(schema)}.
   */
  public void browseSchema() {
    DatabaseMeta databaseMeta = jobMeta.findDatabase( getConfig().getDatabase() );
    Database database = new Database( jobMeta.getParent(), databaseMeta );
    try {
      database.connect();
      String[] schemas = database.getSchemas();

      if ( null != schemas && schemas.length > 0 ) {
        schemas = Const.sortStrings( schemas );
        EnterSelectionDialog dialog =
          new EnterSelectionDialog( getShell(), schemas,
              BaseMessages.getString( AbstractSqoopJobEntry.class, "AvailableSchemas.Title" ),
              BaseMessages.getString( AbstractSqoopJobEntry.class, "AvailableSchemas.Message" ) );
        String schema = dialog.open();
        if ( schema != null ) {
          getConfig().setSchema( schema );
        }
      } else {
        showErrorDialog( BaseMessages.getString( AbstractSqoopJobEntry.class, "Dialog.Error" ), BaseMessages.getString(
            AbstractSqoopJobEntry.class, "NoSchema.Error" ) );
      }
    } catch ( Exception e ) {
      showErrorDialog( BaseMessages.getString( BaseMessages.getString( AbstractSqoopJobEntry.class,
          "System.Dialog.Error.Title" ) ), BaseMessages.getString( BaseMessages.getString( AbstractSqoopJobEntry.class,
          "ErrorRetrievingSchemas" ) ), e );
    } finally {
      database.disconnect();
    }
  }

  /**
   * Show the Database Explorer Dialog for the database information provided. The provided schema and table will be
   * selected if already configured. Any new selection will be saved in the current configuration.
   */
  public void browseTable() {
    DatabaseMeta databaseMeta = jobMeta.findDatabase( getConfig().getDatabase() );
    DatabaseExplorerDialog std =
        new DatabaseExplorerDialog( getShell(), SWT.NONE, databaseMeta, jobMeta.getDatabases() );
    std.setSelectedSchemaAndTable( getConfig().getSchema(), getConfig().getTable() );
    if ( std.open() ) {
      getConfig().setSchema( std.getSchemaName() );
      getConfig().setTable( std.getTableName() );
    }
  }

  protected FileObject getInitialFile( String path ) throws FileSystemException {
    if ( Const.isEmpty( path ) ) {
      path = "/";
    }

    NamedCluster namedCluster =
        jobEntry.getNamedClusterService().getNamedClusterByName( selectedNamedCluster.getName(), getMetaStore() );
    if ( namedCluster == null  ) {
      return null;
    }
    path = namedCluster.processURLsubstitution( path, getMetaStore(), jobEntry );

    FileObject initialFile = null;

    if ( path != null ) {
      String fileName = jobEntry.environmentSubstitute( path );
      if ( fileName != null && !fileName.equals( "" ) ) {
        try {
          initialFile = KettleVFS.getFileObject( fileName );
          if ( namedCluster.isMapr() ) {
            if ( !initialFile.getName().getScheme().startsWith( Schemes.MAPRFS_SCHEME ) ) {
              return null;
            }
          } else if ( !initialFile.getName().getScheme().startsWith( Schemes.HDFS_SCHEME ) ) {
            return null;
          }
        } catch ( Exception ex ) {
          return null;
        }
      }
    }

    return initialFile;
  }

  private IMetaStore getMetaStore() {
    return Spoon.getInstance().getMetaStore();
  }

  protected void extractNamedClusterFromVfsFileChooser() {
    VfsFileChooserDialog dialog = Spoon.getInstance().getVfsFileChooserDialog( null, null );
    CustomVfsUiPanel currentPanel = dialog.getCurrentPanel();
    if ( currentPanel != null && currentPanel instanceof HadoopVfsFileChooserDialog ) {
      HadoopVfsFileChooserDialog hadoopVfsFileChooserDialog = (HadoopVfsFileChooserDialog) currentPanel;
      selectedNamedCluster = hadoopVfsFileChooserDialog.getNamedClusterWidget().getSelectedNamedCluster();
    }
    if ( selectedNamedCluster != null ) {
      setSelectedNamedCluster( selectedNamedCluster );
    }
  }

  public void newCustomArgument() {
    config.getCustomArguments().add( new PropertyEntry() );
    updateDeleteButton();
  }

  public void deleteCustomArgument() {
    XulTree customVariablesTree = (XulTree) container.getDocumentRoot().getElementById( "custom-table" );
    Collection<PropertyEntry> selectedItems = customVariablesTree.getSelectedItems();
    for ( PropertyEntry pe : selectedItems ) {
      try {
        getConfig().getCustomArguments().remove( pe );
      } catch ( Exception e ) {
        customVariablesTree.setElements( getConfig().getCustomArguments() );
      }
    }
    updateDeleteButton();
  }

  public void newNamedCluster( String stepName ) {
    XulDialog xulDialog = (XulDialog) getXulDomContainer().getDocumentRoot().getElementById( stepName );
    Shell shell = (Shell) xulDialog.getRootObject();
    String newNamedCluster = ncDelegate.newNamedCluster( jobMeta, null, shell );
    if ( newNamedCluster != null ) {
      //cancel button on editing pressed, clusters not changed
      populateNamedClusters();
      selectNamedCluster( newNamedCluster );
    }
  }

  public void editNamedCluster( String stepName ) {
    if ( isSelectedNamedCluster() ) {
      XulDialog xulDialog = (XulDialog) getXulDomContainer().getDocumentRoot().getElementById( stepName );
      Shell shell = (Shell) xulDialog.getRootObject();
      String clusterName = ncDelegate.editNamedCluster( null, selectedNamedCluster, shell );
      if ( clusterName != null ) {
        //cancel button on editing pressed, clusters not changed
        populateNamedClusters();
        selectNamedCluster( clusterName );
      }
    }
  }

  public void selectNamedCluster( String configName ) {
    @SuppressWarnings( "unchecked" )
    XulMenuList<NamedCluster> namedConfigMenu =
      (XulMenuList<NamedCluster>) container.getDocumentRoot().getElementById( "named-clusters" );
    for ( NamedCluster nc : getNamedClusters() ) {
      if ( configName != null && configName.equals( nc.getName() ) ) {
        namedConfigMenu.setSelectedItem( nc );
      }
    }
  }

}
