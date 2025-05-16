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


package org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.tree;

import com.google.common.collect.ImmutableMap;
import com.pentaho.big.data.bundles.impl.shim.hdfs.HadoopFileSystemFactoryImpl;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Tree;
import org.pentaho.big.data.api.cluster.service.locator.impl.NamedClusterServiceLocatorImpl;
import org.pentaho.big.data.impl.cluster.NamedClusterManager;
import org.pentaho.big.data.impl.cluster.tests.hdfs.GatewayListHomeDirectoryTest;
import org.pentaho.big.data.impl.cluster.tests.hdfs.GatewayListRootDirectoryTest;
import org.pentaho.big.data.impl.cluster.tests.hdfs.GatewayPingFileSystemEntryPoint;
import org.pentaho.big.data.impl.cluster.tests.hdfs.GatewayWriteToAndDeleteFromUsersHomeFolderTest;
import org.pentaho.big.data.impl.cluster.tests.kafka.KafkaConnectTest;
import org.pentaho.big.data.impl.cluster.tests.mr.GatewayPingJobTrackerTest;
import org.pentaho.big.data.impl.cluster.tests.oozie.GatewayPingOozieHostTest;
import org.pentaho.big.data.impl.cluster.tests.zookeeper.GatewayPingZookeeperEnsembleTest;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.dialog.HadoopClusterDelegate;
import org.pentaho.big.data.kettle.plugins.hadoopcluster.ui.endpoints.HadoopClusterManager;
import org.pentaho.bigdata.api.hdfs.impl.HadoopFileSystemLocatorImpl;
import org.pentaho.di.core.extension.ExtensionPoint;
import org.pentaho.di.core.extension.ExtensionPointInterface;
import org.pentaho.di.core.hadoop.HadoopConfigurationBootstrap;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.namedcluster.model.NamedCluster;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.ui.core.ConstUI;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.spoon.TreeSelection;
import org.pentaho.hadoop.shim.HadoopConfiguration;
import org.pentaho.hadoop.shim.HadoopConfigurationLocator;
import org.pentaho.hadoop.shim.api.ConfigurationException;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.impl.RuntimeTesterImpl;
import org.pentaho.runtime.test.network.impl.ConnectivityTestFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.pentaho.di.i18n.BaseMessages.getString;

@ExtensionPoint( id = "HadoopClusterPopupMenuExtension", description = "Creates popup menus for Hadoop clusters",
  extensionPointId = "SpoonPopupMenuExtension" )
public class HadoopClusterPopupMenuExtension implements ExtensionPointInterface {

  private static final Class<?> PKG = HadoopClusterPopupMenuExtension.class;

  public static final String IMPORT_STATE = "import";
  public static final String NEW_EDIT_STATE = "new-edit";
  public static final String TESTING_STATE = "testing";
  public static final String ADD_DRIVER_STATE = "add-driver";
  public static final String DELETE_STATE = "delete";
  private static final int RESULT_YES = 0;

  private Supplier<Spoon> spoonSupplier = Spoon::getInstance;
  private Menu rootMenu;
  private Menu itemMenu;
  private HadoopClusterDelegate hadoopClusterDelegate;
  private NamedClusterService namedClusterService;
  private String internalShim;
  private static final Logger logChannel = LoggerFactory.getLogger( HadoopClusterPopupMenuExtension.class );
  private NamedCluster lastNamedCluster;
  private RuntimeTester runtimeTester = RuntimeTesterImpl.getInstance();
  private HadoopClusterManager hadoopClusterManager;

  public HadoopClusterPopupMenuExtension() {
    this.namedClusterService = NamedClusterManager.getInstance();
    this.hadoopClusterDelegate = new HadoopClusterDelegate( this.namedClusterService, runtimeTester );
    this.internalShim = "";
    this.hadoopClusterManager =
      new HadoopClusterManager( spoonSupplier.get(), namedClusterService, spoonSupplier.get().getMetaStore(), internalShim );
  }

  public HadoopClusterPopupMenuExtension() {
    this.namedClusterService = NamedClusterManager.getInstance();
    this.hadoopClusterDelegate = new HadoopClusterDelegate( this.namedClusterService, runtimeTester );
    this.internalShim = "";
    this.hadoopClusterManager =
      new HadoopClusterManager( spoonSupplier.get(), namedClusterService, spoonSupplier.get().getMetaStore(), internalShim );
  }

  public HadoopClusterPopupMenuExtension() {
    this.namedClusterService = NamedClusterManager.getInstance();
    this.hadoopClusterDelegate = new HadoopClusterDelegate( this.namedClusterService, runtimeTester );
    this.internalShim = "";
  }

  private void initializeRuntimeTests( RuntimeTester runtimeTester ) {
    try {
    //HadoopConfigurationInfo info = new HadoopConfigurationInfo( "", "", true, true);
    /*
    String identifier = hadoopConfiguration.getIdentifier(); //MIGHT NOT BE NEEDED
    HadoopShim hadoopShim = hadoopConfiguration.getHadoopShim(); //MIGHT NOT BE NEEDED
    String name = hadoopConfiguration.getName(); //MIGHT NOT BE NEEDED
    String id = hadoopConfiguration.getIdentifier(); //MIGHT NOT BE NEEDED
    String version = hadoopShim.getHadoopVersion(); //MIGHT NOT BE NEEDED
    Properties properties = hadoopConfiguration.getConfigProperties(); //MIGHT NOT BE NEEDED

    //REFER TO THE FOLLOWING blueprint.xml IN ORDER TO CREATE AND INITALIZE ShimIdentifier AND HadoopFileSystemFactoryImpl
    //https://github.com/pentaho/pentaho-hadoop-shims/blob/master/shims/cdpdc71/driver/src/main/resources/OSGI-INF/blueprint/blueprint.xml

    //Hierarchy
    //HadoopShim (cdpdc71) -> HadoopShimImpl -> CommonHadoopShim -> HadoopShim
    //https://github.com/pentaho/pentaho-hadoop-shims/blob/master/common-fragment-V1/src/main/java/org/pentaho/hadoop/shim/common/HadoopShimImpl.java
    //https://github.com/pentaho/pentaho-hadoop-shims/blob/master/common-fragment-V1/src/main/java/org/pentaho/hadoop/shim/common/ConfigurationProxyV2.java

    ShimIdentifier shimIdentifier = new ShimIdentifier( "id", "vendor", hadoopShim.getHadoopVersion(), null );
    HadoopFileSystemFactoryImpl hadoopFileSystemFactory = new HadoopFileSystemFactoryImpl( hadoopConfiguration.getHadoopShim(), shimIdentifier );*/
    // Put it in ArrayList and feed it to the HadoopFileSystemLocatorImpl

    //HadoopFileSystemLocatorImpl hadoopFileSystemLocator = new HadoopFileSystemLocatorImpl(  );

    //To add the following runtimeTests it is necessary to add the dependency
    //pentaho:pentaho-big-data-impl-clusterTests
    //and this causes a cyclic reference with pentaho-big-data-impl-clusterTests

    //Runtime tests taken from here:
    //https://github.com/e-cuellar/big-data-plugin/blob/master/impl/clusterTests/src/main/resources/OSGI-INF/blueprint/blueprint.xml
    HadoopConfigurationBootstrap hadoopConfigurationBootstrap = HadoopConfigurationBootstrap.getInstance();
    HadoopConfigurationLocator hadoopConfigurationProvider = (HadoopConfigurationLocator) hadoopConfigurationBootstrap.getProvider();
    HadoopConfiguration hadoopConfiguration = hadoopConfigurationProvider.getActiveConfiguration();

    String activeNamedClusterName = System.getProperty( "ACTIVE_NAMED_CLUSTER" );
    org.pentaho.hadoop.shim.api.cluster.NamedCluster activeNamedCluster =
      namedClusterService.getNamedClusterByName( activeNamedClusterName, spoonSupplier.get().getMetaStore() );
    Configuration configuration = hadoopConfiguration.getHadoopShim().createConfiguration( activeNamedCluster );

    HadoopFileSystemFactory hadoopFileSystemFactory =
      new HadoopFileSystemFactoryImpl( hadoopConfiguration.getHadoopShim(), hadoopConfiguration.getHadoopShim().getShimIdentifier() );

    List<HadoopFileSystemFactory> hadoopFileSystemFactoryList = new ArrayList<>();
    hadoopFileSystemFactoryList.add( hadoopFileSystemFactory );
    HadoopFileSystemLocatorImpl hadoopFileSystemLocator = new HadoopFileSystemLocatorImpl( hadoopFileSystemFactoryList );

    runtimeTester.addRuntimeTest( new GatewayPingFileSystemEntryPoint(  BaseMessagesMessageGetterFactoryImpl.getInstance(), new ConnectivityTestFactoryImpl() ) );
    runtimeTester.addRuntimeTest( new GatewayPingJobTrackerTest(  BaseMessagesMessageGetterFactoryImpl.getInstance(), new ConnectivityTestFactoryImpl() ) );
    runtimeTester.addRuntimeTest( new GatewayPingOozieHostTest(  BaseMessagesMessageGetterFactoryImpl.getInstance(), new ConnectivityTestFactoryImpl() ) );
    runtimeTester.addRuntimeTest( new GatewayPingZookeeperEnsembleTest(  BaseMessagesMessageGetterFactoryImpl.getInstance(), new ConnectivityTestFactoryImpl() ) );

    runtimeTester.addRuntimeTest( new GatewayListRootDirectoryTest( BaseMessagesMessageGetterFactoryImpl.getInstance(), new ConnectivityTestFactoryImpl(), hadoopFileSystemLocator ) );
    runtimeTester.addRuntimeTest( new GatewayListHomeDirectoryTest( BaseMessagesMessageGetterFactoryImpl.getInstance(), new ConnectivityTestFactoryImpl(), hadoopFileSystemLocator ) );
    runtimeTester.addRuntimeTest( new GatewayWriteToAndDeleteFromUsersHomeFolderTest( BaseMessagesMessageGetterFactoryImpl.getInstance(), hadoopFileSystemLocator ) );

    runtimeTester.addRuntimeTest( new KafkaConnectTest( BaseMessagesMessageGetterFactoryImpl.getInstance(), new NamedClusterServiceLocatorImpl( "", NamedClusterManager.getInstance() ) ) );

    } catch ( ConfigurationException e ) {
      throw new RuntimeException( e );
    }
  }

  public HadoopClusterPopupMenuExtension( HadoopClusterDelegate hadoopClusterDelegate,
                                          NamedClusterService namedClusterService, String internalShim ) {
    this.hadoopClusterDelegate = hadoopClusterDelegate;
    this.namedClusterService = namedClusterService;
    this.internalShim = internalShim;
  }

  public void callExtensionPoint( LogChannelInterface log, Object extension ) {
    final Tree selectionTree = (Tree) extension;
    createNewPopupMenu( selectionTree );
  }

  private void createNewPopupMenu( final Tree selectionTree ) {

    Menu popupMenu = null;

    TreeSelection[] objects = spoonSupplier.get().getTreeObjects( selectionTree );
    if ( objects.length != 1 ) {
      return;
    }

    TreeSelection object = objects[ 0 ];
    Object selection = object.getSelection();

    if ( selection instanceof Class<?> && selection.equals( NamedCluster.class ) ) {
      popupMenu = createRootPopupMenu( selectionTree );
    } else if ( selection instanceof NamedCluster ) {
      popupMenu = createMaintPopupMenu( selectionTree, (NamedCluster) selection );
    }

    if ( popupMenu != null ) {
      ConstUI.displayMenu( popupMenu, selectionTree );
    } else {
      selectionTree.setMenu( null );
    }
  }

  private Menu createRootPopupMenu( final Tree tree ) {
    if ( !showAdminFunctions() ) {
      return null;
    }
    if ( rootMenu == null ) {
      rootMenu = new Menu( tree );
      createPopupMenuItem( rootMenu, getString( PKG, "HadoopClusterPopupMenuExtension.MenuItem.New" ),
        NEW_EDIT_STATE );
      createPopupMenuItem( rootMenu, getString( PKG, "HadoopClusterPopupMenuExtension.MenuItem.Import" ),
        IMPORT_STATE );
      createPopupMenuItem( rootMenu, getString( PKG, "HadoopClusterPopupMenuExtension.MenuItem.Add.Driver" ),
        ADD_DRIVER_STATE );
    }
    return rootMenu;
  }

  public Menu createMaintPopupMenu( final Tree selectionTree, NamedCluster namedCluster ) {
    // don't create another menu if the current one is for this namedCluster,
    // otherwise we can see extra pop-up menus.
    if ( itemMenu == null || !namedCluster.equals( this.lastNamedCluster ) ) {
      this.lastNamedCluster = namedCluster;
      itemMenu = new Menu( selectionTree );
      try {
        String name = URLEncoder.encode( namedCluster.getName(), "UTF-8" );
        System.setProperty( "ACTIVE_NAMED_CLUSTER", name );

        initializeRuntimeTests( runtimeTester );

        if ( showAdminFunctions() ) {
          createPopupMenuItem( itemMenu, getString( PKG, "HadoopClusterPopupMenuExtension.MenuItem.Edit" ),
            NEW_EDIT_STATE, ImmutableMap.of( "name", name ) );

          createPopupMenuItem( itemMenu, getString( PKG, "HadoopClusterPopupMenuExtension.MenuItem.Duplicate" ),
            NEW_EDIT_STATE, ImmutableMap.of( "name", name, "duplicateName",
              getString( PKG, "HadoopClusterPopupMenuExtension.Duplicate.Prefix" ) + name ) );
        }
        createPopupMenuItem( itemMenu, getString( PKG, "HadoopClusterPopupMenuExtension.MenuItem.Test" ),
          TESTING_STATE, ImmutableMap.of( "name", name ) );

        if ( showAdminFunctions() ) {
          createDeleteMenuItem( itemMenu, getString( PKG, "HadoopClusterPopupMenuExtension.MenuItem.Delete" ), name );
        }
      } catch ( UnsupportedEncodingException e ) {
        logChannel.error( e.getMessage() );
      }
    }
    return itemMenu;
  }

  private boolean showAdminFunctions() {
    Repository repo = spoonSupplier.get().getRepository();
    if ( repo != null && repo.getUri().isPresent() ) {
      return repo.getSecurityProvider().getUserInfo().isAdmin();
    }
    return true;
  }

  private void createPopupMenuItem( Menu menu, String menuItemLabel, String state ) {
    createPopupMenuItem( menu, menuItemLabel, state, Collections.emptyMap() );
  }

  private void createPopupMenuItem( Menu menu, String menuItemLabel, String state, Map urlParams ) {
    MenuItem menuItem = new MenuItem( menu, SWT.NONE );
    menuItem.setText( menuItemLabel );
    menuItem.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        hadoopClusterDelegate.openDialog( state, urlParams );
      }
    } );
  }

  private void createDeleteMenuItem( Menu menu, String menuItemLabel, String namedCluster ) {
    MenuItem menuItem = new MenuItem( menu, SWT.NONE );
    menuItem.setText( menuItemLabel );
    menuItem.addSelectionListener( new SelectionAdapter() {
      @Override
      public void widgetSelected( SelectionEvent selectionEvent ) {
        try {
          String nCluster = URLDecoder.decode( namedCluster, "UTF-8" );
          String title = BaseMessages.getString( PKG, "PopupMenuFactory.NAMEDCLUSTERS.DeleteNamedClusterAsk.Title" );
          String message =
            BaseMessages.getString( PKG, "PopupMenuFactory.NAMEDCLUSTERS.DeleteNamedClusterAsk.Message",
              nCluster );
          String deleteButton =
            BaseMessages.getString( PKG, "PopupMenuFactory.NAMEDCLUSTERS.DeleteNamedClusterAsk.Delete" );
          String doNotDeleteButton =
            BaseMessages.getString( PKG, "PopupMenuFactory.NAMEDCLUSTERS.DeleteNamedClusterAsk.DoNotDelete" );
          MessageDialog dialog =
            new MessageDialog( spoonSupplier.get().getShell(), title, null, message, MessageDialog.WARNING,
              new String[] {
                deleteButton, doNotDeleteButton }, 0 );
          int response = dialog.open();
          if ( response != RESULT_YES ) {
            return;
          }
          hadoopClusterManager.deleteNamedCluster( spoonSupplier.get().getMetaStore(), nCluster, true );
        } catch ( UnsupportedEncodingException e ) {
          logChannel.error( e.getMessage() );
        }
      }
    } );
  }

}