/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.plugins.common.ui;

import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.FocusListener;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.layout.RowLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Text;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.widget.TextVar;

public class NamedClusterComposite extends Composite {

  private static final String NAMED_CLUSTER_DFS_SCHEME = "named.cluster.dfs.scheme.";

  private static Class<?> PKG = NamedClusterComposite.class;

  private PropsUI props;

  private GridData gridData;
  private GridData numberGridData;
  private GridData labelGridData;
  private GridData userNameLabelGridData;
  private GridData userNameGridData;
  private GridData passwordLabelGridData;
  private GridData passwordGridData;
  private GridData portLabelGridData;

  private static final int ONE_COLUMN = 1;
  private static final int TWO_COLUMNS = 2;

  private static final int TEXT_FLAGS = SWT.SINGLE | SWT.LEFT | SWT.BORDER;
  private static final int PASSWORD_FLAGS = TEXT_FLAGS | SWT.PASSWORD;

  private Text nameValue;

  private Label jtHostLabel;
  private TextVar jtHostNameText;
  private Label jtPortLabel;
  private TextVar jtPortText;
  private Group hdfsGroup;
  private Label hdfsHostLabel;
  private TextVar hdfsHostText;
  private Label hdfsPortLabel;
  private TextVar hdfsPortText;
  private Label hdfsUsernameLabel;
  private TextVar hdfsUsernameText;
  private Label hdfsPasswordLabel;
  private TextVar hdfsPasswordText;
  private Label storageLabel;

  private ArrayList<String> schemeValues = new ArrayList<>();
  private ArrayList<String> schemeNames = new ArrayList<>();

  private interface Callback {
    public void invoke( NamedCluster nc, TextVar textVar, String value );
  }

  public NamedClusterComposite( Composite parent, NamedCluster namedCluster, PropsUI props,
      NamedClusterService namedClusterService ) {
    super( parent, SWT.NONE );
    props.setLook( this );
    this.props = props;

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = 10;
    formLayout.marginHeight = 0;
    setLayout( formLayout );

    FormData fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    setLayoutData( fd );

    gridData = new GridData();
    gridData.widthHint = 270;

    numberGridData = new GridData();
    numberGridData.widthHint = 80;

    labelGridData = new GridData();
    labelGridData.widthHint = 270;

    portLabelGridData = new GridData();
    portLabelGridData.widthHint = 80;

    userNameLabelGridData = new GridData();
    userNameLabelGridData.widthHint = 165;

    userNameGridData = new GridData();
    userNameGridData.widthHint = 165;

    passwordLabelGridData = new GridData();
    passwordLabelGridData.widthHint = 185;

    passwordGridData = new GridData();
    passwordGridData.widthHint = 185;

    processNamedCluster( this, namedCluster, namedClusterService );

    nameValue.forceFocus();
  }

  private void processNamedCluster( final Composite c, final NamedCluster cluster,
      final NamedClusterService namedClusterService ) {

    Composite confUI = createConfigurationUI( c, cluster );

    // Create a horizontal separator
    Label topSeparator = new Label( c, SWT.HORIZONTAL | SWT.SEPARATOR );
    FormData fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( confUI, 10 );
    topSeparator.setLayoutData( fd );

    // Create a storage type Label
    storageLabel = new Label( c, SWT.NONE );
    storageLabel.setText( BaseMessages.getString( PKG, "NamedClusterDialog.Storage" ) );
    storageLabel.setLayoutData( fd );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    fd.top = new FormAttachment( topSeparator, 10 );
    props.setLook( storageLabel );
    storageLabel.setLayoutData( fd );

    // Create a storage type Drop Down
    final CCombo storageCombo = new CCombo( c, SWT.BORDER );
    fd = new FormData();
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 80, 0 );
    fd.top = new FormAttachment( storageLabel, 10 );

    Map<String, Object> properties = namedClusterService.getProperties();
    for ( String key : properties.keySet() ) {
      if ( key.startsWith( NAMED_CLUSTER_DFS_SCHEME ) ) {
        // will add 1 because we should use the key without "."
        schemeValues.add( key.substring( key.lastIndexOf( "." ) + 1  ) );
        schemeNames.add( (String) properties.get( key ) );
      }
    }

    // if we have undefined scheme ( set by variable for example) than we should add the new scheme
    if ( !schemeValues.contains( cluster.getStorageScheme() ) ) {
      schemeValues.add( cluster.getStorageScheme() );
      schemeNames.add( cluster.getStorageScheme() );
    }

    storageCombo.setLayoutData( fd );
    props.setLook( storageCombo );
    storageCombo.setItems( schemeNames.toArray( new String[schemeNames.size()] ) );
    storageCombo.select( schemeValues.indexOf( cluster.getStorageScheme() ) );

    storageCombo.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent e ) {
        super.widgetSelected( e );
        int index = storageCombo.getSelectionIndex();
        if ( index == -1 ) {
          index = 0;
        }
        cluster.setStorageScheme( schemeValues.get( index ) );
        setHdfsAndJobTrackerState( cluster );
      }
    } );
    storageCombo.addFocusListener( new FocusListener() {

      @Override
      public void focusLost( FocusEvent e ) {
        String uiInputText = storageCombo.getText();
        int selectedIndex;
        if ( schemeNames.contains( uiInputText ) ) {
          selectedIndex = schemeNames.indexOf( uiInputText );
          cluster.setStorageScheme( schemeValues.get( selectedIndex ) );
          storageCombo.select( selectedIndex );
        } else if ( schemeValues.contains( uiInputText ) ) {
          selectedIndex = schemeValues.indexOf( uiInputText );
          cluster.setStorageScheme( schemeValues.get( selectedIndex ) );
          storageCombo.select( selectedIndex );
        } else {
          schemeNames.add( uiInputText );
          schemeValues.add( uiInputText );
          storageCombo.setItems( schemeNames.toArray( new String[schemeNames.size()] ) );
          cluster.setStorageScheme( uiInputText );
        }
        setHdfsAndJobTrackerState( cluster );
      }

      @Override
      public void focusGained( FocusEvent e ) {
        // should not do any thing on enter focus
      }
    } );

    // Create a child composite to hold the controls
    final Composite c1 = new Composite( c, SWT.NONE );
    fd = new FormData();
    fd.top = new FormAttachment( storageCombo, 10 );
    fd.left = new FormAttachment( 0, 0 );
    fd.right = new FormAttachment( 100, 0 );
    c1.setLayoutData( fd );
    props.setLook( c1 );
    GridLayout gl = new GridLayout( ONE_COLUMN, false );

    gl.marginHeight = 0;
    gl.marginWidth = 0;

    c1.setLayout( gl );

    createHdfsGroup( c1, cluster );
    createJobTrackerGroup( c1, cluster );
    createZooKeeperGroup( c1, cluster );
    createOozieGroup( c1, cluster );
    createGatewayGroup( c1, cluster );

    c1.setSize( c1.computeSize( SWT.DEFAULT, SWT.DEFAULT ) );

    setHdfsAndJobTrackerState( cluster );
  }

  private Composite createConfigurationUI( final Composite c, final NamedCluster namedCluster ) {
    Composite mainParent = new Composite( c, SWT.NONE );
    props.setLook( mainParent );
    GridLayout gl = new GridLayout( ONE_COLUMN, false );
    gl.marginWidth = 0;

    mainParent.setLayout( gl );
    FormData fd = new FormData();
    mainParent.setLayoutData( fd );

    createLabel( mainParent, BaseMessages.getString( PKG, "NamedClusterDialog.NamedCluster.Name" ), labelGridData );

    nameValue = new Text( mainParent, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    nameValue.setText( String.valueOf( namedCluster.getName() ) );
    nameValue.setLayoutData( gridData );
    props.setLook( nameValue );
    nameValue.addModifyListener( new ModifyListener() {
      public void modifyText( final ModifyEvent modifyEvent ) {
        namedCluster.setName( nameValue.getText() );
      }
    } );
    return mainParent;
  }

  private Label createLabel( Composite parent, String text, GridData gd ) {
    Label label = new Label( parent, SWT.NONE );
    label.setText( text );
    label.setLayoutData( gd );
    props.setLook( label );
    return label;
  }

  private TextVar createTextVar( final NamedCluster c, Composite parent, String val, GridData gd, int flags,
      final Callback cb ) {
    final TextVar textVar = new TextVar( c, parent, flags );
    // SWT will typically not allow a null text
    textVar.setText( StringUtils.isEmpty( val ) ? StringUtils.EMPTY : val );
    textVar.setLayoutData( gd );
    props.setLook( textVar );

    textVar.addModifyListener( new ModifyListener() {
      public void modifyText( final ModifyEvent modifyEvent ) {
        cb.invoke( c, textVar, textVar.getText() );
      }
    } );

    return textVar;
  }

  private Composite createGroup( Composite parent, String groupLabel ) {
    Group group = new Group( parent, SWT.NONE );
    group.setText( groupLabel );
    group.setLayout( new RowLayout( SWT.VERTICAL ) );
    props.setLook( group );
    GridData groupGridData = new GridData();
    groupGridData.grabExcessHorizontalSpace = true;
    groupGridData.horizontalAlignment = SWT.FILL;

    group.setLayoutData( groupGridData );
    // property parent composite
    Composite pp = new Composite( group, SWT.NONE );
    props.setLook( pp );
    GridLayout gridLayout = new GridLayout( ONE_COLUMN, false );
    gridLayout.verticalSpacing = -10;
    gridLayout.marginWidth = 0;

    gridLayout.marginLeft = 5;
    gridLayout.marginRight = 5;
    gridLayout.marginTop = -10;
    gridLayout.marginBottom = -5;

    pp.setLayout( gridLayout );
    return pp;
  }

  private Composite createTwoColumnsContainer( Composite parentComposite ) {
    Composite twoColumnsComposite = new Composite( parentComposite, SWT.NONE );
    props.setLook( twoColumnsComposite );
    GridLayout gridLayout = new GridLayout( TWO_COLUMNS, false );
    gridLayout.marginWidth = 0;
    twoColumnsComposite.setLayout( gridLayout );
    return twoColumnsComposite;
  }

  private void createHdfsGroup( Composite parentComposite, final NamedCluster c ) {
    Composite pp = createGroup( parentComposite, BaseMessages.getString( PKG, "NamedClusterDialog.HDFS" ) );
    hdfsGroup = (Group) pp.getParent();
    Composite hdfsRowComposite = createTwoColumnsContainer( pp );
    Composite hostUIComposite = new Composite( hdfsRowComposite, SWT.NONE );
    props.setLook( hostUIComposite );
    hostUIComposite.setLayout( new GridLayout( ONE_COLUMN, false ) );

    Composite portUIComposite = new Composite( hdfsRowComposite, SWT.NONE );
    props.setLook( portUIComposite );
    portUIComposite.setLayout( new GridLayout( ONE_COLUMN, false ) );

    hdfsHostLabel = createLabel( hostUIComposite, BaseMessages.getString( PKG, "NamedClusterDialog.Hostname" ), labelGridData );
    // hdfs host input
    Callback hdfsHostCB = new Callback() {
      public void invoke( NamedCluster nc, TextVar textVar, String value ) {
        nc.setHdfsHost( value );
      }
    };
    hdfsHostText = createTextVar( c, hostUIComposite, c.getHdfsHost(), gridData, TEXT_FLAGS, hdfsHostCB );

    hdfsPortLabel =  createLabel( portUIComposite, BaseMessages.getString( PKG, "NamedClusterDialog.Port" ), portLabelGridData );
    // hdfs port input
    Callback hdfsPortCB = new Callback() {
      public void invoke( NamedCluster nc, TextVar textVar, String value ) {
        nc.setHdfsPort( value );
      }
    };
    hdfsPortText = createTextVar( c, portUIComposite, c.getHdfsPort(), numberGridData, TEXT_FLAGS, hdfsPortCB );

    Composite hdfsCredentialsRowComposite = createTwoColumnsContainer( pp );

    Composite usernameUIComposite = new Composite( hdfsCredentialsRowComposite, SWT.NONE );
    props.setLook( usernameUIComposite );
    usernameUIComposite.setLayout( new GridLayout( ONE_COLUMN, false ) );

    Composite passwordUIComposite = new Composite( hdfsCredentialsRowComposite, SWT.NONE );
    props.setLook( passwordUIComposite );
    passwordUIComposite.setLayout( new GridLayout( ONE_COLUMN, false ) );

    hdfsUsernameLabel =
        createLabel( usernameUIComposite, BaseMessages.getString( PKG, "NamedClusterDialog.Username" ),
            userNameLabelGridData );
    // hdfs user input
    Callback hdfsUsernameCB = new Callback() {
      public void invoke( NamedCluster nc, TextVar textVar, String value ) {
        nc.setHdfsUsername( value );
      }
    };
    hdfsUsernameText =
        createTextVar( c, usernameUIComposite, c.getHdfsUsername(), userNameGridData, TEXT_FLAGS, hdfsUsernameCB );

    hdfsPasswordLabel =
        createLabel( passwordUIComposite, BaseMessages.getString( PKG, "NamedClusterDialog.Password" ),
            passwordLabelGridData );
    // hdfs password input
    Callback hdfsPasswordCB = new Callback() {
      public void invoke( NamedCluster nc, TextVar textVar, String value ) {
        nc.setHdfsPassword( value );
      }
    };
    hdfsPasswordText =
        createTextVar( c, passwordUIComposite, c.getHdfsPassword(), passwordGridData, PASSWORD_FLAGS, hdfsPasswordCB );

  }

  private void createJobTrackerGroup( Composite parentComposite, final NamedCluster c ) {
    Composite pp = createGroup( parentComposite, BaseMessages.getString( PKG, "NamedClusterDialog.JobTracker" ) );

    Composite jobTrackerRowComposite = createTwoColumnsContainer( pp );

    Composite hostUIComposite = new Composite( jobTrackerRowComposite, SWT.NONE );
    props.setLook( hostUIComposite );
    hostUIComposite.setLayout( new GridLayout( ONE_COLUMN, false ) );

    Composite portUIComposite = new Composite( jobTrackerRowComposite, SWT.NONE );
    props.setLook( portUIComposite );
    portUIComposite.setLayout( new GridLayout( ONE_COLUMN, false ) );

    jtHostLabel =
        createLabel( hostUIComposite, BaseMessages.getString( PKG, "NamedClusterDialog.Hostname" ), labelGridData );
    // hdfs host input
    Callback hostCB = new Callback() {
      public void invoke( NamedCluster nc, TextVar textVar, String value ) {
        nc.setJobTrackerHost( value );
      }
    };
    jtHostNameText = createTextVar( c, hostUIComposite, c.getJobTrackerHost(), gridData, TEXT_FLAGS, hostCB );

    jtPortLabel =
        createLabel( portUIComposite, BaseMessages.getString( PKG, "NamedClusterDialog.Port" ), portLabelGridData );
    // hdfs port input
    Callback portCB = new Callback() {
      public void invoke( NamedCluster nc, TextVar textVar, String value ) {
        nc.setJobTrackerPort( value );
      }
    };
    jtPortText = createTextVar( c, portUIComposite, c.getJobTrackerPort(), numberGridData, TEXT_FLAGS, portCB );
  }

  private void createZooKeeperGroup( Composite parentComposite, final NamedCluster c ) {
    Composite pp = createGroup( parentComposite, BaseMessages.getString( PKG, "NamedClusterDialog.ZooKeeper" ) );

    Composite zooKeeperRowComposite = createTwoColumnsContainer( pp );

    Composite hostUIComposite = new Composite( zooKeeperRowComposite, SWT.NONE );
    props.setLook( hostUIComposite );
    hostUIComposite.setLayout( new GridLayout( ONE_COLUMN, false ) );

    Composite portUIComposite = new Composite( zooKeeperRowComposite, SWT.NONE );
    props.setLook( portUIComposite );
    portUIComposite.setLayout( new GridLayout( ONE_COLUMN, false ) );

    // hdfs host label
    createLabel( hostUIComposite, BaseMessages.getString( PKG, "NamedClusterDialog.Hostname" ), labelGridData );
    // hdfs host input
    Callback hostCB = new Callback() {
      public void invoke( NamedCluster nc, TextVar textVar, String value ) {
        nc.setZooKeeperHost( value );
      }
    };
    createTextVar( c, hostUIComposite, c.getZooKeeperHost(), gridData, TEXT_FLAGS, hostCB );

    // hdfs port label
    createLabel( portUIComposite, BaseMessages.getString( PKG, "NamedClusterDialog.Port" ), portLabelGridData );
    // hdfs port input
    Callback portCB = new Callback() {
      public void invoke( NamedCluster nc, TextVar textVar, String value ) {
        nc.setZooKeeperPort( value );
      }
    };
    createTextVar( c, portUIComposite, c.getZooKeeperPort(), numberGridData, TEXT_FLAGS, portCB );
  }

  private void createOozieGroup( Composite parentComposite, final NamedCluster c ) {
    Composite pp = createGroup( parentComposite, BaseMessages.getString( PKG, "NamedClusterDialog.Oozie" ) );

    Composite container = new Composite( pp, SWT.NONE );
    props.setLook( container );
    GridLayout gridLayout = new GridLayout( ONE_COLUMN, false );
    gridLayout.marginBottom = 5;
    gridLayout.marginTop = 5;

    container.setLayout( gridLayout );

    // oozie label
    createLabel( container, BaseMessages.getString( PKG, "NamedClusterDialog.URL" ), labelGridData );
    // oozie url
    Callback hostCB = new Callback() {
      public void invoke( NamedCluster nc, TextVar textVar, String value ) {
        nc.setOozieUrl( value );
      }
    };
    createTextVar( c, container, c.getOozieUrl(), gridData, TEXT_FLAGS, hostCB );
  }

  private void createGatewayGroup( Composite parentComposite, final NamedCluster c ) {
    Composite pp = createGroup( parentComposite, BaseMessages.getString( PKG, "NamedClusterDialog.Gateway" ) );
    GridLayout gridLayout = new GridLayout( ONE_COLUMN, false );
    gridLayout.marginBottom = 5;
    gridLayout.marginTop = 5;
    Composite gatewayUrlUIComposite = new Composite( pp, SWT.NONE );
    props.setLook( gatewayUrlUIComposite );
    gatewayUrlUIComposite.setLayout( gridLayout );

    createLabel( gatewayUrlUIComposite, BaseMessages.getString( PKG, "NamedClusterDialog.GatewayUrl" ), labelGridData );
    // gateway url input
    Callback gatewayUrlCB = new Callback() {
      public void invoke( NamedCluster nc, TextVar textVar, String value ) {
        nc.setGatewayUrl( value );
      }
    };
    createTextVar( c, gatewayUrlUIComposite, c.getGatewayUrl(), gridData, TEXT_FLAGS, gatewayUrlCB );

    Composite gatewayCredentialsRowComposite = createTwoColumnsContainer( pp );

    Composite usernameUIComposite = new Composite( gatewayCredentialsRowComposite, SWT.NONE );
    props.setLook( usernameUIComposite );
    usernameUIComposite.setLayout( new GridLayout( ONE_COLUMN, false ) );

    Composite passwordUIComposite = new Composite( gatewayCredentialsRowComposite, SWT.NONE );
    props.setLook( passwordUIComposite );
    passwordUIComposite.setLayout( new GridLayout( ONE_COLUMN, false ) );

    createLabel( usernameUIComposite, BaseMessages.getString( PKG, "NamedClusterDialog.Username" ),  userNameLabelGridData );
    // gateway user input
    Callback gatewayUsernameCB = new Callback() {
      public void invoke( NamedCluster nc, TextVar textVar, String value ) {
        nc.setGatewayUsername( value );
      }
    };
    createTextVar( c, usernameUIComposite, c.getGatewayUsername(), userNameGridData, TEXT_FLAGS, gatewayUsernameCB );

    createLabel( passwordUIComposite, BaseMessages.getString( PKG, "NamedClusterDialog.Password" ), passwordLabelGridData );
    // gateway password input
    Callback gatewayPasswordCB = new Callback() {
      public void invoke( NamedCluster nc, TextVar textVar, String value ) {
        nc.setGatewayPassword( value );
      }
    };
    createTextVar( c, passwordUIComposite, c.getGatewayPassword(), passwordGridData, PASSWORD_FLAGS, gatewayPasswordCB );
  }

  private void setHdfsAndJobTrackerState( NamedCluster cluster ) {
    boolean state = !cluster.isMapr();
    jtHostLabel.setEnabled( state );
    jtHostNameText.setEnabled( state );
    jtPortLabel.setEnabled( state );
    jtPortText.setEnabled( state );
    hdfsHostLabel.setEnabled( state );
    hdfsHostText.setEnabled( state );
    hdfsPortLabel.setEnabled( state );
    hdfsPortText.setEnabled( state );
    hdfsUsernameLabel.setEnabled( state );
    hdfsUsernameText.setEnabled( state );
    hdfsPasswordLabel.setEnabled( state );
    hdfsPasswordText.setEnabled( state );
    String storageName = cluster.getStorageScheme();
    //get the human readable name
    if ( !Utils.isEmpty( schemeNames ) && !Utils.isEmpty( schemeValues ) ) {
      storageName = schemeNames.get( schemeValues.indexOf( storageName ) );
    }
    hdfsGroup.setText( storageName );
  }
}
