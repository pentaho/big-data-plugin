/*! ******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2023 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.amazon;

import com.google.common.base.Strings;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.amazon.client.ClientFactoriesManager;
import org.pentaho.amazon.client.ClientType;
import org.pentaho.amazon.client.api.AimClient;
import org.pentaho.amazon.client.api.PricingClient;
import org.pentaho.amazon.client.api.S3Client;
import org.pentaho.amazon.s3.S3VfsFileChooserHelper;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.plugins.JobEntryPluginType;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.util.StringUtil;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.database.dialog.tags.ExtTextbox;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.util.HelpUtils;
import org.pentaho.ui.xul.XulDomContainer;
import org.pentaho.ui.xul.XulException;
import org.pentaho.ui.xul.binding.Binding;
import org.pentaho.ui.xul.binding.BindingConvertor;
import org.pentaho.ui.xul.binding.BindingFactory;
import org.pentaho.ui.xul.components.XulButton;
import org.pentaho.ui.xul.components.XulMenuList;
import org.pentaho.ui.xul.components.XulRadio;
import org.pentaho.ui.xul.components.XulRadioGroup;
import org.pentaho.ui.xul.components.XulTextbox;
import org.pentaho.ui.xul.containers.XulDeck;
import org.pentaho.ui.xul.containers.XulDialog;
import org.pentaho.ui.xul.impl.AbstractXulEventHandler;
import org.pentaho.ui.xul.swt.tags.SwtDialog;
import org.pentaho.ui.xul.util.AbstractModelList;
import org.pentaho.vfs.ui.VfsFileChooserDialog;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public abstract class AbstractAmazonJobExecutorController extends AbstractXulEventHandler {

  private static final Class<?> PKG = AbstractAmazonJobExecutorController.class;

  /* property change names */
  public static final String JOB_ENTRY_NAME = "jobEntryName";
  public static final String HADOOP_JOB_NAME = "hadoopJobName";
  public static final String HADOOP_JOB_FLOW_ID = "hadoopJobFlowId";
  public static final String JAR_URL = "jarUrl";
  public static final String ACCESS_KEY = "accessKey";
  public static final String SECRET_KEY = "secretKey";
  public static final String SESSION_TOKEN = "sessionToken";
  public static final String STAGING_DIR = "stagingDir";
  public static final String STAGING_DIR_FILE = "stagingDirFile";
  public static final String NUM_INSTANCES = "numInstances";
  public static final String REGION = "region";
  public static final String MASTER_INSTANCE_TYPE = "masterInstanceType";
  public static final String SLAVE_INSTANCE_TYPE = "slaveInstanceType";
  public static final String EC2_ROLE = "ec2Role";
  public static final String EMR_ROLE = "emrRole";
  public static final String CMD_LINE_ARGS = "commandLineArgs";
  public static final String BLOCKING = "blocking";
  public static final String RUN_ON_NEW_CLUSTER = "runOnNewCluster";
  public static final String LOGGING_INTERVAL = "loggingInterval";
  public static final String ALIVE = "alive";

  /* XUL Element id's */
  public static final String XUL_JOBENTRY_NAME = "jobentry-name";
  public static final String XUL_JOBENTRY_HADOOPJOB_NAME = "jobentry-hadoopjob-name";
  public static final String XUL_ACCESS_KEY = "access-key";
  public static final String XUL_SECRET_KEY = "secret-key";
  public static final String XUL_SESSION_TOKEN = "session-token";
  public static final String XUL_EMR_SETTINGS = "emr-settings";
  public static final String XUL_REGION = "region";
  public static final String XUL_EC2_ROLE = "ec2-role";
  public static final String XUL_EMR_ROLE = "emr-role";
  public static final String XUL_MASTER_INSTANCE_TYPE = "master-instance-type";
  public static final String XUL_SLAVE_INSTANCE_TYPE = "slave-instance-type";
  public static final String XUL_EMR_RELEASE = "emr-release";
  public static final String XUL_JOBENTRY_HADOOPJOB_FLOW_ID = "jobentry-hadoopjob-flow-id";
  public static final String XUL_S3_STAGING_DIRECTORY = "s3-staging-directory";
  public static final String XUL_COMMAND_LINE_ARGUMENTS = "command-line-arguments";
  public static final String XUL_NUM_INSTANCES = "num-instances";
  public static final String XUL_BLOCKING = "blocking";
  public static final String XUL_LOGGING_INTERVAL1 = "logging-interval";
  public static final String XUL_ALIVE = "alive";
  public static final String XUL_AMAZON_EMR_JOB_ENTRY_DIALOG = "amazon-emr-job-entry-dialog";
  public static final String XUL_AMAZON_EMR_ERROR_DIALOG = "amazon-emr-error-dialog";
  public static final String XUL_AMAZON_EMR_ERROR_MESSAGE = "amazon-emr-error-message";
  public static final String XUL_CLUSTER_TAB = "cluster-tab";
  public static final String XUL_NEW_CLUSTER_DECK = "new-cluster";
  public static final String XUL_EXISTING_CLUSTER_DECK = "existing-cluster";
  public static final String XUL_CLUSTER_MODE = "cluster-mode";

  private static final String EC2_DEFAULT_ROLE = "EMR_EC2_DefaultRole";
  private static final String EMR_DEFAULT_ROLE = "EMR_DefaultRole";

  private static final String DISABLED_FLAG = "disabled";
  private static final String BOOLEAN_TO_STR_CONVERSION_ERROR = "Boolean to String conversion is not supported";

  protected static final String[] XUL_EMR_MENU_ID_ARRAY =
    { XUL_EC2_ROLE, XUL_EMR_ROLE, XUL_MASTER_INSTANCE_TYPE, XUL_SLAVE_INSTANCE_TYPE, XUL_EMR_RELEASE };

  protected String jobEntryName;
  protected String hadoopJobName;
  protected String hadoopJobFlowId;
  protected String accessKey = "";
  protected String secretKey = "";
  protected String sessionToken = "";

  protected String stagingDir = "";
  protected FileObject stagingDirFile = null;
  protected String jarUrl = "";
  protected boolean alive = false;

  protected String numInstances = "2";
  protected String masterInstanceType;
  protected String slaveInstanceType;

  protected String region;
  protected String emrRelease;

  protected String ec2Role;
  protected String emrRole;

  protected String commandLineArgs;
  protected boolean blocking;
  protected boolean runOnNewCluster;
  protected String loggingInterval = "60"; // 60 seconds

  protected VfsFileChooserDialog fileChooserDialog;
  protected S3VfsFileChooserHelper helper;

  // Generically typed fields
  protected AbstractAmazonJobEntry jobEntry; // AbstractJobEntry<BlockableJobConfig>

  // common fields
  protected XulDomContainer container;
  protected BindingFactory bindingFactory;
  protected List<Binding> bindings;

  private AbstractModelList<String> masterInstanceTypes;
  private AbstractModelList<String> slaveInstanceTypes;
  private AbstractModelList<String> regions;
  private AbstractModelList<String> ec2Roles;
  private AbstractModelList<String> emrRoles;
  private AbstractModelList<String> releases;

  protected boolean suppressEventHandling = false;

  public AbstractAmazonJobExecutorController( XulDomContainer container, AbstractAmazonJobEntry jobEntry,
                                              BindingFactory bindingFactory ) {

    this.jobEntry = jobEntry;
    this.container = container;
    this.bindingFactory = bindingFactory;

    regions = new AbstractModelList<>();
    ec2Roles = new AbstractModelList<>();
    emrRoles = new AbstractModelList<>();
    masterInstanceTypes = new AbstractModelList<>();
    slaveInstanceTypes = new AbstractModelList<>();
    releases = new AbstractModelList<>();
    bindings = new ArrayList<>();
  }

  public AbstractAmazonJobExecutorController() {
  }

  protected void initializeEmrSettingsGroupMenuFields() {
    populateRegions();
    populateEc2Roles();
    populateEmrRoles();
    populateMasterInstanceTypes();
    populateSlaveInstanceTypes();
    populateReleases();
  }

  protected void initializeTextFields() {

    XulTextbox numInstances = (XulTextbox) container.getDocumentRoot().getElementById( XUL_NUM_INSTANCES );
    numInstances.setValue( getNumInstances() );
    XulTextbox loggingInterval =
      (XulTextbox) container.getDocumentRoot().getElementById( XUL_LOGGING_INTERVAL1 );
    loggingInterval.setValue( getLoggingInterval() );

    ExtTextbox tempBox = (ExtTextbox) container.getDocumentRoot().getElementById( XUL_ACCESS_KEY );
    tempBox.setVariableSpace( getVariableSpace() );
    tempBox = (ExtTextbox) container.getDocumentRoot().getElementById( XUL_SECRET_KEY );
    tempBox.setVariableSpace( getVariableSpace() );
    tempBox = (ExtTextbox) container.getDocumentRoot().getElementById( XUL_SESSION_TOKEN );
    tempBox.setVariableSpace( getVariableSpace() );
    tempBox = (ExtTextbox) container.getDocumentRoot().getElementById( XUL_JOBENTRY_HADOOPJOB_NAME );
    tempBox.setVariableSpace( getVariableSpace() );
    tempBox = (ExtTextbox) container.getDocumentRoot().getElementById( XUL_JOBENTRY_HADOOPJOB_FLOW_ID );
    tempBox.setVariableSpace( getVariableSpace() );
    tempBox = (ExtTextbox) container.getDocumentRoot().getElementById( XUL_S3_STAGING_DIRECTORY );
    tempBox.setVariableSpace( getVariableSpace() );
    tempBox = (ExtTextbox) container.getDocumentRoot().getElementById( XUL_COMMAND_LINE_ARGUMENTS );
    tempBox.setVariableSpace( getVariableSpace() );
    tempBox = (ExtTextbox) container.getDocumentRoot().getElementById( XUL_NUM_INSTANCES );
    tempBox.setVariableSpace( getVariableSpace() );
    tempBox = (ExtTextbox) container.getDocumentRoot().getElementById( XUL_LOGGING_INTERVAL1 );
    tempBox.setVariableSpace( getVariableSpace() );
  }

  protected void createBindings() {

    bindingFactory.setBindingType( Binding.Type.BI_DIRECTIONAL );

    bindings.add( bindingFactory.createBinding( regions, "children", XUL_REGION, "elements" ) );
    bindingFactory.createBinding( XUL_REGION, "selectedIndex", this, "selectedRegion",
      new BindingConvertor<Integer, String>() {
        public String sourceToTarget( final Integer index ) {
          if ( index == -1 ) {
            return null;
          }
          return regions.get( index );
        }

        public Integer targetToSource( final String str ) {
          return regions.indexOf( str );
        }
      } );

    bindings.add( bindingFactory.createBinding( ec2Roles, "children", XUL_EC2_ROLE, "elements" ) );
    bindingFactory.createBinding( XUL_EC2_ROLE, "selectedIndex", this, "selectedEc2Role",
      new BindingConvertor<Integer, String>() {
        public String sourceToTarget( final Integer index ) {
          if ( index == -1 ) {
            return null;
          }
          return ec2Roles.get( index );
        }

        public Integer targetToSource( final String role ) {
          return ec2Roles.indexOf( role );
        }
      } );

    bindings.add( bindingFactory.createBinding( emrRoles, "children", XUL_EMR_ROLE, "elements" ) );
    bindingFactory.createBinding( XUL_EMR_ROLE, "selectedIndex", this, "selectedEmrRole",
      new BindingConvertor<Integer, String>() {
        public String sourceToTarget( final Integer index ) {
          if ( index == -1 ) {
            return null;
          }
          return emrRoles.get( index );
        }

        public Integer targetToSource( final String role ) {
          return emrRoles.indexOf( role );
        }
      } );

    bindings
      .add( bindingFactory.createBinding( masterInstanceTypes, "children", XUL_MASTER_INSTANCE_TYPE, "elements" ) );
    bindingFactory.createBinding( XUL_MASTER_INSTANCE_TYPE, "selectedIndex", this, "selectedMasterInstanceType",
      new BindingConvertor<Integer, String>() {
        public String sourceToTarget( final Integer index ) {
          if ( index == -1 ) {
            return null;
          }
          return masterInstanceTypes.get( index );
        }

        public Integer targetToSource( final String str ) {
          return masterInstanceTypes.indexOf( str );
        }
      } );

    bindings.add( bindingFactory.createBinding( slaveInstanceTypes, "children", XUL_SLAVE_INSTANCE_TYPE, "elements" ) );
    bindingFactory.createBinding( XUL_SLAVE_INSTANCE_TYPE, "selectedIndex", this, "selectedSlaveInstanceType",
      new BindingConvertor<Integer, String>() {
        public String sourceToTarget( final Integer index ) {
          if ( index == -1 ) {
            return null;
          }
          return slaveInstanceTypes.get( index );
        }

        public Integer targetToSource( final String str ) {
          return slaveInstanceTypes.indexOf( str );
        }
      } );

    bindings.add( bindingFactory.createBinding( releases, "children", XUL_EMR_RELEASE, "elements" ) );
    bindingFactory.createBinding( XUL_EMR_RELEASE, "selectedIndex", this, "emrRelease",
      new BindingConvertor<Integer, String>() {
        public String sourceToTarget( final Integer index ) {
          if ( index == -1 ) {
            return null;
          }
          return releases.get( index );
        }

        public Integer targetToSource( final String str ) {
          return releases.indexOf( str );
        }
      } );

    bindingFactory.createBinding( XUL_JOBENTRY_NAME, "value", this, JOB_ENTRY_NAME );
    bindingFactory.createBinding( XUL_JOBENTRY_HADOOPJOB_NAME, "value", this, HADOOP_JOB_NAME );
    bindingFactory.createBinding( XUL_JOBENTRY_HADOOPJOB_FLOW_ID, "value", this, HADOOP_JOB_FLOW_ID );
    bindingFactory.createBinding( XUL_ACCESS_KEY, "value", this, ACCESS_KEY );
    bindingFactory.createBinding( XUL_SECRET_KEY, "value", this, SECRET_KEY );
    bindingFactory.createBinding( XUL_SESSION_TOKEN, "value", this, SESSION_TOKEN );
    bindingFactory.createBinding( XUL_S3_STAGING_DIRECTORY, "value", this, STAGING_DIR );
    bindingFactory.createBinding( XUL_COMMAND_LINE_ARGUMENTS, "value", this, CMD_LINE_ARGS );
    bindingFactory.createBinding( XUL_NUM_INSTANCES, "value", this, NUM_INSTANCES );
    bindingFactory.createBinding( XUL_ALIVE, "selected", this, ALIVE );
    bindingFactory.createBinding( XUL_BLOCKING, "selected", this, BLOCKING );
    bindingFactory.createBinding( XUL_LOGGING_INTERVAL1, "value", this, LOGGING_INTERVAL );

    bindingFactory.setBindingType( Binding.Type.ONE_WAY );
    bindingFactory
      .createBinding( XUL_ACCESS_KEY, "value", XUL_EMR_SETTINGS, DISABLED_FLAG, secretKeyIsEmpty( container ) );
    bindingFactory
      .createBinding( XUL_SECRET_KEY, "value", XUL_EMR_SETTINGS, DISABLED_FLAG, accessKeyIsEmpty( container ) );
    bindingFactory
            .createBinding( XUL_SESSION_TOKEN, "value", XUL_EMR_SETTINGS, DISABLED_FLAG, sessionTokenIsEmpty( container ) );
  }

  private static void disableAwsConnection( XulDomContainer container ) {
    XulButton connectButton = (XulButton) container.getDocumentRoot().getElementById( XUL_EMR_SETTINGS );
    connectButton.setDisabled( disableConnectButton( container ) );
  }

  public void updateClusterState() {
    XulRadioGroup clusterModes =
      (XulRadioGroup) getXulDomContainer().getDocumentRoot().getElementById( XUL_CLUSTER_MODE );
    XulRadio newClusterMode = (XulRadio) clusterModes.getFirstChild();

    XulDeck clusterModeTab = (XulDeck) getXulDomContainer().getDocumentRoot().getElementById( XUL_CLUSTER_TAB );
    disableAwsConnection( getXulDomContainer() );

    if ( newClusterMode.isSelected() ) {
      this.runOnNewCluster = true;
      clusterModeTab.setSelectedIndex( 0 );
    } else {
      fixFocusLostOnTab();
      clusterModeTab.setSelectedIndex( 1 );
      this.runOnNewCluster = false;
    }
  }

  //need for mac os to avoid getting NullPointerException after switching to Existing tab
  private void fixFocusLostOnTab() {
    XulTextbox jobEntryName = (XulTextbox) getXulDomContainer().getDocumentRoot().getElementById( XUL_JOBENTRY_NAME );
    jobEntryName.setFocus();
  }

  private static String getTextBoxValueById( XulDomContainer container, String xulTextBoxName ) {
    ExtTextbox textbox = (ExtTextbox) container.getDocumentRoot().getElementById( xulTextBoxName );
    return textbox.getValue();
  }

  private static boolean disableConnectButton( XulDomContainer container ) {
    String secretKeyValue = getTextBoxValueById( container, XUL_SECRET_KEY );
    String accessKeyValue = getTextBoxValueById( container, XUL_ACCESS_KEY );
    XulRadio existingClusterMode = (XulRadio) container.getDocumentRoot().getElementById( XUL_EXISTING_CLUSTER_DECK );

    if ( existingClusterMode.isSelected() ) {
      return true;
    }

    if ( Strings.isNullOrEmpty( accessKeyValue ) || Strings.isNullOrEmpty( secretKeyValue ) ) {
      return true;
    }

    return false;
  }

  private static BindingConvertor<String, Boolean> accessKeyIsEmpty( XulDomContainer container ) {
    return new BindingConvertor<String, Boolean>() {
      @Override public Boolean sourceToTarget( String value ) {
        return disableConnectButton( container );
      }

      @Override public String targetToSource( Boolean value ) {
        throw new AbstractMethodError( BOOLEAN_TO_STR_CONVERSION_ERROR );
      }
    };
  }

  private static BindingConvertor<String, Boolean> secretKeyIsEmpty( XulDomContainer container ) {
    return new BindingConvertor<String, Boolean>() {
      @Override public Boolean sourceToTarget( String value ) {
        return disableConnectButton( container );
      }

      @Override public String targetToSource( Boolean value ) {
        throw new AbstractMethodError( BOOLEAN_TO_STR_CONVERSION_ERROR );
      }
    };
  }

  private static BindingConvertor<String, Boolean> sessionTokenIsEmpty( XulDomContainer container ) {
    return new BindingConvertor<String, Boolean>() {
      @Override public Boolean sourceToTarget( String value ) {
        return disableConnectButton( container );
      }

      @Override public String targetToSource( Boolean value ) {
        throw new AbstractMethodError( BOOLEAN_TO_STR_CONVERSION_ERROR );
      }
    };
  }

  protected AbstractModelList<String> populateEc2Roles() {
    String ec2Role = getJobEntry().getEc2Role();
    if ( ec2Role != null ) {
      ec2Roles.add( ec2Role );
    }
    return ec2Roles;
  }

  protected AbstractModelList<String> populateEmrRoles() {
    String emrRole = getJobEntry().getEmrRole();
    if ( emrRole != null ) {
      emrRoles.add( emrRole );
    }
    return emrRoles;
  }

  protected AbstractModelList<String> populateMasterInstanceTypes() {
    String masterInstanceType = getJobEntry().getMasterInstanceType();
    if ( masterInstanceType != null ) {
      masterInstanceTypes.add( masterInstanceType );
    }
    return masterInstanceTypes;
  }

  protected AbstractModelList<String> populateSlaveInstanceTypes() {
    String slaveInstanceType = getJobEntry().getSlaveInstanceType();
    if ( slaveInstanceType != null ) {
      slaveInstanceTypes.add( slaveInstanceType );
    }
    return slaveInstanceTypes;
  }

  protected void setRolesFromAmazonAccount( AimClient amiClient ) throws Exception {
    setEc2RolesFromAmazonAccount( amiClient );
    setEmrRolesFromAmazonAccount( amiClient );
  }

  private void setEc2RolesFromAmazonAccount( AimClient amiClient ) {
    AbstractModelList<String> ec2List;
    ec2List = amiClient.getEc2RolesFromAmazonAccount();

    if ( ec2List.isEmpty() ) {
      ec2List.add( EC2_DEFAULT_ROLE );
    }

    ec2Roles.clear();
    ec2Roles.addAll( ec2List );
  }

  private void setEmrRolesFromAmazonAccount( AimClient amiClient ) {
    AbstractModelList<String> emrList;
    emrList = amiClient.getEmrRolesFromAmazonAccount();

    if ( emrList.isEmpty() ) {
      emrList.add( EMR_DEFAULT_ROLE );
    }

    emrRoles.clear();
    emrRoles.addAll( emrList );
  }

  protected List<String> populateInstanceTypesForSelectedRegion( PricingClient pricingClient ) throws Exception {

    List<String> instanceTypes = null;

    try {
      instanceTypes = pricingClient.populateInstanceTypesForSelectedRegion();
    } catch ( IOException e ) {
      e.printStackTrace();
      showErrorDialog(
        BaseMessages.getString( PKG, "AbstractAmazonJobExecutorController.JobEntry.Instance.error.title" ),
        e.getMessage() );
    }

    masterInstanceTypes.clear();
    slaveInstanceTypes.clear();

    if ( instanceTypes != null ) {
      masterInstanceTypes.addAll( instanceTypes );
      slaveInstanceTypes.addAll( instanceTypes );
    }

    return instanceTypes;
  }

  protected AbstractModelList<String> populateRegions() {
    regions.clear();
    regions =
      Arrays.stream( AmazonRegion.values() ).map( v -> v.getHumanReadableRegion() )
        .collect( Collectors.toCollection( AbstractModelList<String>::new ) );

    String region = getJobEntry().getRegion();

    if ( region == null && regions.size() > 0 ) {
      getJobEntry().setRegion( regions.get( 0 ) );
    }

    return regions;
  }

  protected AbstractModelList<String> populateReleases() {
    releases.clear();
    releases =
      Arrays.stream( AmazonEmrReleases.values() ).map( v -> v.getEmrRelease() )
        .collect( Collectors.toCollection( AbstractModelList<String>::new ) );

    String emrRelease = getJobEntry().getEmrRelease();

    if ( emrRelease != null && !releases.contains( emrRelease ) ) {
      releases.add( 0, emrRelease );
    }

    if ( emrRelease == null && releases.size() > 0 ) {
      getJobEntry().setEmrRelease( releases.get( 0 ) );
    }
    return releases;
  }

  public AbstractModelList<String> getReleases() {
    return releases;
  }

  private XulMenuList<String> getXulMenu( String elementMenuId ) {
    return (XulMenuList<String>) getXulDomContainer().getDocumentRoot().getElementById( elementMenuId );
  }

  private void setXulMenuDisabled( String elementMenuId, boolean isDisable ) {
    XulMenuList<String> xulMenu = getXulMenu( elementMenuId );
    xulMenu.setDisabled( isDisable );
  }

  protected void setXulMenusDisabled( boolean isDisable ) {
    Arrays.stream( XUL_EMR_MENU_ID_ARRAY ).forEach( ( e ) -> setXulMenuDisabled( e, isDisable ) );
  }

  protected void setSelectedItemForEachMenu() {
    getXulMenu( XUL_EC2_ROLE ).setSelectedItem( getJobEntry().getEc2Role() );
    getXulMenu( XUL_EMR_ROLE ).setSelectedItem( getJobEntry().getEmrRole() );
    getXulMenu( XUL_MASTER_INSTANCE_TYPE ).setSelectedItem( getJobEntry().getMasterInstanceType() );
    getXulMenu( XUL_SLAVE_INSTANCE_TYPE ).setSelectedItem( getJobEntry().getSlaveInstanceType() );
  }

  private List<String> initRolesAndTypes( String accessKey, String secretKey, String sessionToken ) throws Exception {

    ClientFactoriesManager manager = ClientFactoriesManager.getInstance();

    AimClient aimClient = manager
      .createClient( accessKey, secretKey, sessionToken, getJobEntry().getRegion(), ClientType.AIM );

    setRolesFromAmazonAccount( aimClient );

    PricingClient pricingClient = manager
      .createClient( accessKey, secretKey, sessionToken, getJobEntry().getRegion(), ClientType.PRICING );

    return populateInstanceTypesForSelectedRegion( pricingClient );
  }

  public void getEmrSettings() {

    ExtTextbox accessKeyBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( XUL_ACCESS_KEY );

    ExtTextbox secretKeyBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( XUL_SECRET_KEY );

    ExtTextbox sessionTokenBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( XUL_SESSION_TOKEN );

    XulButton connectButton = (XulButton) getXulDomContainer().getDocumentRoot().getElementById( XUL_EMR_SETTINGS );

    connectButton.setDisabled( true );

    String ec2Role = getJobEntry().getEc2Role();
    String emrRole = getJobEntry().getEmrRole();
    String masterSelectedInstanceType = getJobEntry().getMasterInstanceType();
    String slaveSelectedInstanceType = getJobEntry().getSlaveInstanceType();
    String errorMessage;

    try {

      List<String> instanceTypes = initRolesAndTypes( accessKeyBox.getValue(), secretKeyBox.getValue(), sessionTokenBox.getValue() );

      if ( ec2Role != null && ec2Roles.contains( ec2Role ) ) {
        this.ec2Role = ec2Role;
      }

      if ( emrRole != null && emrRoles.contains( emrRole ) ) {
        this.emrRole = emrRole;
      }

      if ( masterSelectedInstanceType != null && instanceTypes.contains( masterSelectedInstanceType ) ) {
        this.masterInstanceType = masterSelectedInstanceType;
      }

      if ( slaveSelectedInstanceType != null && instanceTypes.contains( slaveSelectedInstanceType ) ) {
        this.slaveInstanceType = slaveSelectedInstanceType;
      }

      setXulMenusDisabled( false );

      XulTextbox numInstances = (ExtTextbox) container.getDocumentRoot().getElementById( XUL_NUM_INSTANCES );
      numInstances.setDisabled( false );

      setSelectedItemForEachMenu();

    } catch ( Exception e ) {
      e.printStackTrace();
      errorMessage = e.getMessage() == null ? ExceptionUtils.getStackTrace( e ) : e.getMessage();
      showErrorDialog(
        BaseMessages.getString( PKG, "AbstractAmazonJobExecutorController.JobEntry.Connection.error.title" ),
        errorMessage );
    } finally {
      connectButton.setDisabled( false );
    }
  }

  public AbstractModelList<String> getEc2Roles() {
    return ec2Roles;
  }

  public AbstractModelList<String> getEmrRoles() {
    return emrRoles;
  }

  public AbstractModelList<String> getMasterInstanceTypes() {
    return masterInstanceTypes;
  }

  public AbstractModelList<String> getSlaveInstanceTypes() {
    return slaveInstanceTypes;
  }

  public AbstractModelList<String> getRegions() {
    return regions;
  }

  public void setBindings( List<Binding> bindings ) {
    this.bindings = bindings;
  }

  public void accept() {
    syncModel();

    String validationErrors = buildValidationErrorMessages();

    if ( !StringUtil.isEmpty( validationErrors ) ) {
      openErrorDialog( BaseMessages.getString( PKG, "Dialog.Error" ), validationErrors );
      return;
    }

    configureJobEntry();

    cancel();
  }

  protected void syncModel() {
    XulMenuList<String> tempMenu =
      (XulMenuList<String>) getXulDomContainer().getDocumentRoot().getElementById( XUL_REGION );
    this.region = tempMenu.getValue();
    tempMenu = (XulMenuList<String>) getXulDomContainer().getDocumentRoot().getElementById( XUL_EC2_ROLE );
    this.ec2Role = tempMenu.getValue();
    tempMenu = (XulMenuList<String>) getXulDomContainer().getDocumentRoot().getElementById( XUL_EMR_ROLE );
    this.emrRole = tempMenu.getValue();
    tempMenu = (XulMenuList<String>) getXulDomContainer().getDocumentRoot().getElementById( XUL_MASTER_INSTANCE_TYPE );
    this.masterInstanceType = tempMenu.getValue();
    tempMenu = (XulMenuList<String>) getXulDomContainer().getDocumentRoot().getElementById( XUL_SLAVE_INSTANCE_TYPE );
    this.slaveInstanceType = tempMenu.getValue();
    tempMenu = (XulMenuList<String>) getXulDomContainer().getDocumentRoot().getElementById( XUL_EMR_RELEASE );
    this.emrRelease = tempMenu.getValue();
    ExtTextbox tempBox =
      (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( XUL_JOBENTRY_HADOOPJOB_NAME );
    this.hadoopJobName = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( XUL_ACCESS_KEY );
    this.accessKey = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( XUL_SECRET_KEY );
    this.secretKey = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( XUL_SESSION_TOKEN );
    this.sessionToken = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( XUL_JOBENTRY_HADOOPJOB_FLOW_ID );
    this.hadoopJobFlowId = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( XUL_S3_STAGING_DIRECTORY );
    this.stagingDir = ( (Text) tempBox.getTextControl() ).getText();
    try {
      this.stagingDirFile = resolveFile( this.stagingDir );
    } catch ( Exception e ) {
      this.stagingDirFile = null;
    }

    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( XUL_COMMAND_LINE_ARGUMENTS );
    this.commandLineArgs = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( XUL_NUM_INSTANCES );
    this.numInstances = ( (Text) tempBox.getTextControl() ).getText();
    tempBox = (ExtTextbox) getXulDomContainer().getDocumentRoot().getElementById( XUL_LOGGING_INTERVAL1 );
    this.loggingInterval = ( (Text) tempBox.getTextControl() ).getText();
  }

  public List<String> getValidationWarnings() {
    List<String> warnings = new ArrayList<String>();

    if ( StringUtil.isEmpty( getJobEntryName() ) ) {
      warnings.add( BaseMessages.getString( PKG, "AbstractAmazonJobExecutorController.JobEntryName.Error" ) );
    }
    if ( StringUtil.isEmpty( getAccessKey() ) ) {
      warnings.add( BaseMessages.getString( PKG, "AbstractAmazonJobExecutorController.AccessKey.Error" ) );
    }
    if ( StringUtil.isEmpty( getSecretKey() ) ) {
      warnings.add( BaseMessages.getString( PKG, "AbstractAmazonJobExecutorController.SecretKey.Error" ) );
    }
    if ( StringUtil.isEmpty( getRegion() ) ) {
      warnings.add( BaseMessages.getString( PKG, "AbstractAmazonJobExecutorController.Region.Error" ) );
    }

    warnings.addAll( collectClusterWarnings() );

    if ( StringUtil.isEmpty( getHadoopJobName() ) ) {
      warnings.add( BaseMessages.getString( PKG, "AbstractAmazonJobExecutorController.JobFlowName.Error" ) );
    }

    String s3Protocol = S3Client.SCHEME + "://";
    String sdir = getVariableSpace().environmentSubstitute( stagingDir );
    if ( StringUtil.isEmpty( getStagingDir() ) ) {
      warnings.add( BaseMessages.getString( PKG, "AbstractAmazonJobExecutorController.StagingDir.Error" ) );
    } else if ( !sdir.startsWith( s3Protocol ) ) {
      warnings.add( BaseMessages.getString( PKG, "AbstractAmazonJobExecutorController.StagingDir.Error" ) );
    }

    return warnings;
  }

  private List<String> collectClusterWarnings() {

    List<String> newClusterWarnings = new ArrayList<>();
    List<String> existingClusterWarnings = new ArrayList<>();

    if ( StringUtil.isEmpty( getEc2Role() ) ) {
      newClusterWarnings.add( BaseMessages.getString( PKG, "AbstractAmazonJobExecutorController.Ec2Role.Error" ) );
    }
    if ( StringUtil.isEmpty( getEmrRole() ) ) {
      newClusterWarnings.add( BaseMessages.getString( PKG, "AbstractAmazonJobExecutorController.EmrRole.Error" ) );
    }
    if ( StringUtil.isEmpty( getMasterInstanceType() ) ) {
      newClusterWarnings
        .add( BaseMessages.getString( PKG, "AbstractAmazonJobExecutorController.MasterInstanceType.Error" ) );
    }
    if ( StringUtil.isEmpty( getSlaveInstanceType() ) ) {
      newClusterWarnings
        .add( BaseMessages.getString( PKG, "AbstractAmazonJobExecutorController.SlaveInstanceType.Error" ) );
    }
    if ( StringUtil.isEmpty( getEmrRelease() ) ) {
      newClusterWarnings.add( BaseMessages.getString( PKG, "AbstractAmazonJobExecutorController.EmrRelease.Error" ) );
    }

    if ( StringUtil.isEmpty( getHadoopJobFlowId() ) ) {
      existingClusterWarnings
        .add( BaseMessages.getString( PKG, "AbstractAmazonJobExecutorController.JobFlowId.Error" ) );
    }

    if ( isRunOnNewCluster() ) {
      return newClusterWarnings;
    }

    return existingClusterWarnings;
  }

  protected String buildValidationErrorMessages() {
    StringBuilder sb = new StringBuilder();
    List<String> warnings = getValidationWarnings();

    if ( !warnings.isEmpty() ) {
      for ( String warning : warnings ) {
        sb.append( warning ).append( "\n" );
      }
    }

    return sb.toString();
  }

  protected void configureJobEntry() {
    // common/simple
    getJobEntry().setName( getJobEntryName() );
    getJobEntry().setHadoopJobName( getHadoopJobName() );
    getJobEntry().setHadoopJobFlowId( getHadoopJobFlowId() );
    getJobEntry().setAccessKey( getAccessKey() );
    getJobEntry().setSecretKey( getSecretKey() );
    getJobEntry().setSessionToken( getSessionToken() );
    getJobEntry().setStagingDir( getStagingDir() );
    getJobEntry().setNumInstances( getNumInstances() );
    getJobEntry().setMasterInstanceType( getMasterInstanceType() );
    getJobEntry().setSlaveInstanceType( getSlaveInstanceType() );
    getJobEntry().setRegion( getRegion() );
    getJobEntry().setEmrRelease( getEmrRelease() );
    getJobEntry().setEc2Role( getEc2Role() );
    getJobEntry().setEmrRole( getEmrRole() );
    getJobEntry().setCmdLineArgs( getCommandLineArgs() );
    getJobEntry().setAlive( isAlive() );
    getJobEntry().setRunOnNewCluster( isRunOnNewCluster() );
    getJobEntry().setBlocking( getBlocking() );
    getJobEntry().setLoggingInterval( getLoggingInterval() );

    getJobEntry().setChanged();
  }


  public String getSelectedEc2Role() {
    return this.ec2Role;
  }

  public String getSelectedEmrRole() {
    return this.emrRole;
  }

  public String getSelectedMasterInstanceType() {
    return this.masterInstanceType;
  }

  public String getSelectedSlaveInstanceType() {
    return this.slaveInstanceType;
  }

  public void setSelectedEc2Role( String selectedEc2Role ) {
    if ( !suppressEventHandling ) {

      if ( this.ec2Role == null && ec2Roles.contains( EC2_DEFAULT_ROLE ) ) {
        selectedEc2Role = EC2_DEFAULT_ROLE;
      }

      suppressEventHandling = true;
      try {
        firePropertyChange( "selectedEc2Role", this.ec2Role, selectedEc2Role );
        this.ec2Role = selectedEc2Role;
      } finally {
        suppressEventHandling = false;
      }
    }
  }

  public void setSelectedEmrRole( String selectedEmrRole ) {
    if ( !suppressEventHandling ) {

      if ( this.emrRole == null && emrRoles.contains( EMR_DEFAULT_ROLE ) ) {
        selectedEmrRole = EMR_DEFAULT_ROLE;
      }

      suppressEventHandling = true;
      try {
        firePropertyChange( "selectedEmrRole", this.emrRole, selectedEmrRole );
        this.emrRole = selectedEmrRole;
      } finally {
        suppressEventHandling = false;
      }
    }
  }

  public void setSelectedMasterInstanceType( String selectedMasterInstanceType ) {

    if ( !suppressEventHandling ) {

      suppressEventHandling = true;
      try {
        firePropertyChange( "selectedMasterInstanceType", this.masterInstanceType, selectedMasterInstanceType );
        this.masterInstanceType = selectedMasterInstanceType;
      } finally {
        suppressEventHandling = false;
      }
    }
  }

  public void setSelectedSlaveInstanceType( String selectedSlaveInstanceType ) {

    if ( !suppressEventHandling ) {

      suppressEventHandling = true;
      try {
        firePropertyChange( "selectedSlaveInstanceType", this.slaveInstanceType, selectedSlaveInstanceType );
        this.slaveInstanceType = selectedSlaveInstanceType;
      } finally {
        suppressEventHandling = false;
      }
    }
  }

  public String getSelectedRegion() {
    return getRegion();
  }

  public void setSelectedRegion( String selectedRegion ) {

    if ( !suppressEventHandling ) {

      if ( !selectedRegion.equals( this.region ) ) {
        setXulMenusDisabled( true );

        ec2Roles.clear();
        emrRoles.clear();
        masterInstanceTypes.clear();
        slaveInstanceTypes.clear();
      }

      this.region = selectedRegion;

      getJobEntry().setRegion( this.region );

      suppressEventHandling = true;
      try {
        firePropertyChange( "selectedRegion", null, this.region );
      } finally {
        suppressEventHandling = false;
      }
    }
  }

  protected void initializeEc2RoleSelection() {
    @SuppressWarnings( "unchecked" )
    XulMenuList<String> ec2RoleMenu = getXulMenu( XUL_EC2_ROLE );
    String selectedEc2Role = getJobEntry().getEc2Role();
    ec2RoleMenu.setSelectedItem( selectedEc2Role );
  }

  protected void initializeEmrRoleSelection() {
    @SuppressWarnings( "unchecked" )
    XulMenuList<String> emrRoleMenu = getXulMenu( XUL_EMR_ROLE );
    String selectedEmrRole = getJobEntry().getEmrRole();
    emrRoleMenu.setSelectedItem( selectedEmrRole );
  }

  protected void initializeMasterInstanceSelection() {
    @SuppressWarnings( "unchecked" )
    XulMenuList<String> namedClusterMenu = getXulMenu( XUL_MASTER_INSTANCE_TYPE );
    String selectedMasterInstanceType = getJobEntry().getMasterInstanceType();
    namedClusterMenu.setSelectedItem( selectedMasterInstanceType );
  }

  protected void initializeSlaveInstanceSelection() {
    @SuppressWarnings( "unchecked" )
    XulMenuList<String> namedClusterMenu = getXulMenu( XUL_SLAVE_INSTANCE_TYPE );
    String selectedSlaveInstanceType = getJobEntry().getSlaveInstanceType();
    namedClusterMenu.setSelectedItem( selectedSlaveInstanceType );
  }

  protected void initializeRegionSelection() {
    @SuppressWarnings( "unchecked" )
    XulMenuList<String> namedClusterMenu = getXulMenu( XUL_REGION );
    String selectedRegion = getJobEntry().getRegion();
    namedClusterMenu.setSelectedItem( selectedRegion );
  }

  protected void initializeReleaseSelection() {
    @SuppressWarnings( "unchecked" )
    XulMenuList<String> namedClusterMenu = getXulMenu( XUL_EMR_RELEASE );
    String selectedRelease = getJobEntry().getEmrRelease();
    namedClusterMenu.setSelectedItem( selectedRelease );
  }

  protected void initializeClusterSelection() {
    XulRadio newCluster = (XulRadio) container.getDocumentRoot().getElementById( XUL_NEW_CLUSTER_DECK );
    XulRadio existingCluster = (XulRadio) container.getDocumentRoot().getElementById( XUL_EXISTING_CLUSTER_DECK );

    newCluster.setSelected( this.runOnNewCluster );
    existingCluster.setSelected( !this.runOnNewCluster );

    updateClusterState();
  }

  protected void beforeInit() {

    suppressEventHandling = true;

    if ( getJobEntry() != null ) {
      setName( getJobEntry().getName() );
      setJobEntryName( getJobEntry().getName() );
      setHadoopJobName( getJobEntry().getHadoopJobName() );
      setHadoopJobFlowId( getJobEntry().getHadoopJobFlowId() );
      setAccessKey( getJobEntry().getAccessKey() );
      setSecretKey( getJobEntry().getSecretKey() );
      setSessionToken( getJobEntry().getSessionToken() );
      setStagingDir( getJobEntry().getStagingDir() );
      setNumInstances( getJobEntry().getNumInstances() );
      setMasterInstanceType( getJobEntry().getMasterInstanceType() );
      setSlaveInstanceType( getJobEntry().getSlaveInstanceType() );
      setRegion( getJobEntry().getRegion() );
      setEmrRelease( getJobEntry().getEmrRelease() );
      setEc2Role( getJobEntry().getEc2Role() );
      setEmrRole( getJobEntry().getEmrRole() );
      setCommandLineArgs( getJobEntry().getCmdLineArgs() );
      setRunOnNewCluster( getJobEntry().isRunOnNewCluster() );
      setBlocking( getJobEntry().getBlocking() );
      setAlive( getJobEntry().getAlive() );
      setLoggingInterval( getJobEntry().getLoggingInterval() );
    }
  }

  protected void afterInit() {

    suppressEventHandling = false;

    initializeRegionSelection();
    initializeEc2RoleSelection();
    initializeEmrRoleSelection();
    initializeMasterInstanceSelection();
    initializeSlaveInstanceSelection();
    initializeReleaseSelection();
    initializeClusterSelection();
  }

  protected abstract String getDialogElementId();

  /**
   * Look up the dialog reference from the document.
   *
   * @return The dialog element referred to by {@link #getDialogElementId()}
   */
  protected SwtDialog getDialog() {
    return (SwtDialog) getXulDomContainer().getDocumentRoot().getElementById( getDialogElementId() );
  }

  /**
   * @return the shell for the currently visible dialog. This will be used to display additional dialogs/popups.
   */
  protected Shell getShell() {
    return getDialog().getShell();
  }

  public JobEntryInterface open() {
    XulDialog dialog = getDialog();
    dialog.show();
    return getJobEntry();
  }

  /**
   * Show an error dialog with the title and message provided.
   *
   * @param title   Dialog window title
   * @param message Dialog message
   */
  protected void showErrorDialog( String title, String message ) {
    MessageBox mb = new MessageBox( getShell(), SWT.OK | SWT.ICON_ERROR );
    mb.setText( title );
    mb.setMessage( message );
    mb.open();
  }

  public void init() throws XulException, InvocationTargetException {

    beforeInit();

    try {
      for ( Binding binding : bindings ) {
        binding.fireSourceChanged();
      }
    } finally {
      afterInit();
    }
  }

  public void cancel() {
    XulDialog xulDialog =
      (XulDialog) getXulDomContainer().getDocumentRoot().getElementById( XUL_AMAZON_EMR_JOB_ENTRY_DIALOG );
    Shell shell = (Shell) xulDialog.getRootObject();
    if ( !shell.isDisposed() ) {
      WindowProperty winprop = new WindowProperty( shell );
      PropsUI.getInstance().setScreen( winprop );
      ( (Composite) xulDialog.getManagedObject() ).dispose();
      shell.dispose();
    }
  }

  public void openErrorDialog( String title, String message ) {
    XulDialog errorDialog =
      (XulDialog) getXulDomContainer().getDocumentRoot().getElementById( XUL_AMAZON_EMR_ERROR_DIALOG );
    errorDialog.setTitle( title );

    XulTextbox errorMessage =
      (XulTextbox) getXulDomContainer().getDocumentRoot().getElementById( XUL_AMAZON_EMR_ERROR_MESSAGE );
    errorMessage.setValue( message );

    errorDialog.show();
  }

  public void closeErrorDialog() {
    XulDialog errorDialog =
      (XulDialog) getXulDomContainer().getDocumentRoot().getElementById( XUL_AMAZON_EMR_ERROR_DIALOG );
    errorDialog.hide();
  }

  protected VfsFileChooserDialog getFileChooserDialog() throws KettleFileException {
    if ( this.fileChooserDialog == null ) {
      FileObject initialFile = null;
      FileObject defaultInitialFile = KettleVFS.getFileObject( "file:///c:/" );

      VfsFileChooserDialog fileChooserDialog =
        Spoon.getInstance().getVfsFileChooserDialog( defaultInitialFile, initialFile );
      this.fileChooserDialog = fileChooserDialog;
    }
    return this.fileChooserDialog;
  }

  protected FileSystemOptions getFileSystemOptions() {
    FileSystemOptions opts = new FileSystemOptions();

    if ( !Const.isEmpty( getAccessKey() ) || !Const.isEmpty( getSecretKey() ) ) {
      // create a FileSystemOptions with user & password
      StaticUserAuthenticator userAuthenticator =
        new StaticUserAuthenticator( null, getVariableSpace().environmentSubstitute( getAccessKey() ),
          getVariableSpace().environmentSubstitute( getSecretKey() ) );

      DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator( opts, userAuthenticator );
    }
    return opts;
  }

  public FileObject browse( String[] fileFilters, String[] fileFilterNames, String fileUri ) throws KettleException,
    FileSystemException {
    return browse( fileFilters, fileFilterNames, fileUri, new FileSystemOptions() );
  }

  public FileObject browse( String[] fileFilters, String[] fileFilterNames, String fileUri, int fileDialogMode )
    throws KettleException, FileSystemException {
    return browse( fileFilters, fileFilterNames, fileUri, new FileSystemOptions(), fileDialogMode );
  }

  public FileObject browse( String[] fileFilters, String[] fileFilterNames, String fileUri, FileSystemOptions opts )
    throws KettleException, FileSystemException {
    return browse( fileFilters, fileFilterNames, fileUri, opts, VfsFileChooserDialog.VFS_DIALOG_OPEN_DIRECTORY );
  }

  public FileObject browse( String[] fileFilters, String[] fileFilterNames, String fileUri, FileSystemOptions opts,
                            int fileDialogMode ) throws KettleException, FileSystemException {
    return browse( fileFilters, fileFilterNames, fileUri, opts, fileDialogMode, false );
  }

  public FileObject browse( String[] fileFilters, String[] fileFilterNames, String fileUri, FileSystemOptions opts,
                            int fileDialogMode, boolean showFileScheme ) throws KettleException, FileSystemException {
    getFileChooserHelper().setShowFileScheme( showFileScheme );
    return getFileChooserHelper().browse( fileFilters, fileFilterNames, fileUri, opts, fileDialogMode );
  }

  public void browseS3StagingDir() throws KettleException, FileSystemException {
    String[] fileFilters = new String[] { "*.*" };
    String[] fileFilterNames = new String[] { "All" };

    String stagingDirText = getVariableSpace().environmentSubstitute( stagingDir );
    FileSystemOptions opts = getFileSystemOptions();

    FileObject selectedFile = browse( fileFilters, fileFilterNames, stagingDirText, opts );

    if ( selectedFile != null ) {
      setStagingDir( selectedFile.getName().getURI() );
    }
  }

  public VariableSpace getVariableSpace() {
    if ( Spoon.getInstance().getActiveTransformation() != null ) {
      return Spoon.getInstance().getActiveTransformation();
    } else if ( Spoon.getInstance().getActiveJob() != null ) {
      return Spoon.getInstance().getActiveJob();
    } else {
      return new Variables();
    }
  }

  @Override
  public String getName() {
    return "jobEntryController";
  }

  public String getJobEntryName() {
    return jobEntryName;
  }

  public void setJobEntryName( String jobEntryName ) {
    String previousVal = this.jobEntryName;
    String newVal = jobEntryName;

    this.jobEntryName = jobEntryName;
    firePropertyChange( JOB_ENTRY_NAME, previousVal, newVal );
  }

  public String getHadoopJobName() {
    return hadoopJobName;
  }

  public void setHadoopJobName( String hadoopJobName ) {
    String previousVal = this.hadoopJobName;
    String newVal = hadoopJobName;

    this.hadoopJobName = hadoopJobName;
    firePropertyChange( HADOOP_JOB_NAME, previousVal, newVal );
  }

  public String getHadoopJobFlowId() {
    return hadoopJobFlowId;
  }

  public void setHadoopJobFlowId( String hadoopJobFlowId ) {
    String previousVal = this.hadoopJobFlowId;
    String newVal = hadoopJobFlowId;

    this.hadoopJobFlowId = hadoopJobFlowId;
    firePropertyChange( HADOOP_JOB_FLOW_ID, previousVal, newVal );
  }

  public String getAccessKey() {
    return accessKey;
  }

  public void setAccessKey( String accessKey ) {
    String previousVal = this.accessKey;
    String newVal = accessKey;

    this.accessKey = accessKey;
    firePropertyChange( ACCESS_KEY, previousVal, newVal );
  }

  public String getSecretKey() {
    return secretKey;
  }

  public void setSecretKey( String secretKey ) {
    String previousVal = this.secretKey;
    String newVal = secretKey;

    this.secretKey = secretKey;
    firePropertyChange( SECRET_KEY, previousVal, newVal );
  }

  public String getSessionToken() {
    return sessionToken;
  }

  public void setSessionToken( String sessionToken ) {
    String previousVal = this.sessionToken;
    String newVal = sessionToken;

    this.sessionToken = sessionToken;
    firePropertyChange( SESSION_TOKEN, previousVal, newVal );
  }

  public String getRegion() {
    return region;
  }

  public void setRegion( String region ) {
    String previousVal = this.region;
    String newVal = region;

    this.region = region;
    firePropertyChange( REGION, previousVal, newVal );
  }

  public String getStagingDir() {
    return stagingDir;
  }

  public void setStagingDir( String stagingDir ) {
    String previousVal = this.stagingDir;
    String newVal = stagingDir;

    this.stagingDir = stagingDir;
    firePropertyChange( STAGING_DIR, previousVal, newVal );
  }

  public FileObject getStagingDirFile() {
    return stagingDirFile;
  }

  public void setStagingDirFile( FileObject stagingDirFile ) {
    FileObject previousVal = this.stagingDirFile;
    FileObject newVal = stagingDirFile;

    this.stagingDirFile = stagingDirFile;
    firePropertyChange( STAGING_DIR_FILE, previousVal, newVal );
  }

  public String getJarUrl() {
    return jarUrl;
  }

  public void setJarUrl( String jarUrl ) {
    String previousVal = this.jarUrl;
    String newVal = jarUrl;

    this.jarUrl = jarUrl;
    firePropertyChange( JAR_URL, previousVal, newVal );
  }

  public String getNumInstances() {
    return numInstances;
  }

  public void setNumInstances( String numInstances ) {
    String previousVal = this.numInstances;
    String newVal = numInstances;

    this.numInstances = numInstances;
    firePropertyChange( NUM_INSTANCES, previousVal, newVal );
  }

  public String getMasterInstanceType() {
    return masterInstanceType;
  }

  public void setMasterInstanceType( String masterInstanceType ) {
    String previousVal = this.masterInstanceType;
    String newVal = masterInstanceType;

    this.masterInstanceType = masterInstanceType;
    firePropertyChange( MASTER_INSTANCE_TYPE, previousVal, newVal );
  }

  public String getSlaveInstanceType() {
    return slaveInstanceType;
  }

  public void setSlaveInstanceType( String slaveInstanceType ) {
    String previousVal = this.slaveInstanceType;
    String newVal = slaveInstanceType;

    this.slaveInstanceType = slaveInstanceType;
    firePropertyChange( SLAVE_INSTANCE_TYPE, previousVal, newVal );
  }

  public String getEc2Role() {
    return ec2Role;
  }

  public void setEc2Role( String ec2Role ) {
    String previousVal = this.ec2Role;
    String newVal = ec2Role;

    this.ec2Role = ec2Role;
    firePropertyChange( EC2_ROLE, previousVal, newVal );
  }

  public String getEmrRole() {
    return emrRole;
  }

  public void setEmrRole( String emrRole ) {
    String previousVal = this.emrRole;
    String newVal = emrRole;

    this.emrRole = emrRole;
    firePropertyChange( EMR_ROLE, previousVal, newVal );
  }

  public void invertBlocking() {
    setBlocking( !getBlocking() );
  }

  public abstract AbstractAmazonJobEntry getJobEntry();

  public abstract void setJobEntry( AbstractAmazonJobEntry jobEntry );

  public String getCommandLineArgs() {
    return commandLineArgs;
  }

  public void setCommandLineArgs( String commandLineArgs ) {
    String previousVal = this.commandLineArgs;
    String newVal = commandLineArgs;

    this.commandLineArgs = commandLineArgs;

    firePropertyChange( CMD_LINE_ARGS, previousVal, newVal );
  }

  public boolean isRunOnNewCluster() {
    return runOnNewCluster;
  }


  public void setRunOnNewCluster( boolean selected ) {
    boolean previousVal = this.runOnNewCluster;
    boolean newVal = selected;

    this.runOnNewCluster = selected;

    firePropertyChange( RUN_ON_NEW_CLUSTER, previousVal, newVal );
  }

  public boolean getBlocking() {
    return blocking;
  }

  public void setBlocking( boolean blocking ) {
    boolean previousVal = this.blocking;
    boolean newVal = blocking;

    this.blocking = blocking;
    firePropertyChange( BLOCKING, previousVal, newVal );
  }

  public String getLoggingInterval() {
    return loggingInterval;
  }

  public void setLoggingInterval( String loggingInterval ) {
    String previousVal = this.loggingInterval;
    String newVal = loggingInterval;

    this.loggingInterval = loggingInterval;
    firePropertyChange( LOGGING_INTERVAL, previousVal, newVal );
  }

  public String getEmrRelease() {
    return emrRelease;
  }

  public void setEmrRelease( String emrRelease ) {

    if ( !suppressEventHandling ) {
      this.emrRelease = emrRelease;
      getJobEntry().setEmrRelease( this.emrRelease );

      suppressEventHandling = true;
      try {
        firePropertyChange( "emrRelease", null, this.emrRelease );
      } finally {
        suppressEventHandling = false;
      }
    }
  }

  public boolean isAlive() {
    return alive;
  }

  public void setAlive( boolean alive ) {
    boolean previousVal = this.alive;
    this.alive = alive;
    firePropertyChange( ALIVE, previousVal, alive );
  }

  public void invertAlive() {
    setAlive( !isAlive() );
  }

  public FileObject resolveFile( String fileUri ) throws KettleFileException {
    VariableSpace vs = getVariableSpace();
    FileSystemOptions opts = new FileSystemOptions();
    DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator( opts,
      new StaticUserAuthenticator( null, getAccessKey(), getSecretKey() ) );
    FileObject file = KettleVFS.getFileObject( fileUri, vs, opts );
    return file;
  }

  protected S3VfsFileChooserHelper getFileChooserHelper() throws KettleFileException, FileSystemException {
    if ( helper == null ) {
      XulDialog xulDialog =
        (XulDialog) getXulDomContainer().getDocumentRoot().getElementById( XUL_AMAZON_EMR_JOB_ENTRY_DIALOG );
      Shell shell = (Shell) xulDialog.getRootObject();

      helper = new S3VfsFileChooserHelper( shell, getFileChooserDialog(), getVariableSpace(), getFileSystemOptions() );
    }
    return helper;
  }

  public void help() {
    JobEntryInterface jobEntry = getJobEntry();
    XulDialog xulDialog =
      (XulDialog) getXulDomContainer().getDocumentRoot().getElementById( XUL_AMAZON_EMR_JOB_ENTRY_DIALOG );
    Shell shell = (Shell) xulDialog.getRootObject();
    PluginInterface plugin =
      PluginRegistry.getInstance().findPluginWithId( JobEntryPluginType.class, jobEntry.getPluginId() );
    HelpUtils.openHelpDialog( shell, plugin );
  }
}
