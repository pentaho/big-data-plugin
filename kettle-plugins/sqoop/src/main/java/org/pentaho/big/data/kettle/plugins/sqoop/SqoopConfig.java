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

package org.pentaho.big.data.kettle.plugins.sqoop;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.big.data.kettle.plugins.job.BlockableJobConfig;
import org.pentaho.big.data.kettle.plugins.job.JobEntryMode;
import org.pentaho.big.data.kettle.plugins.job.Password;
import org.pentaho.big.data.kettle.plugins.job.PropertyEntry;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.ui.xul.XulEventSource;
import org.pentaho.ui.xul.util.AbstractModelList;
import org.w3c.dom.Node;

import java.util.Map;

/**
 * A collection of configuration objects for a Sqoop job entry.
 */
public abstract class SqoopConfig extends BlockableJobConfig implements XulEventSource, Cloneable {
  public static final String NAMENODE_HOST = "namenodeHost";
  public static final String NAMENODE_PORT = "namenodePort";
  public static final String JOBTRACKER_HOST = "jobtrackerHost";
  public static final String JOBTRACKER_PORT = "jobtrackerPort";

  public static final String DATABASE = "database";
  public static final String SCHEMA = "schema";

  // Common arguments
  public static final String CONNECT = "connect";
  public static final String USERNAME = "username";
  public static final String PASSWORD = "password";
  public static final String VERBOSE = "verbose";
  public static final String CONNECTION_MANAGER = "connectionManager";
  public static final String DRIVER = "driver";
  public static final String CONNECTION_PARAM_FILE = "connectionParamFile";
  public static final String HADOOP_HOME = "hadoopHome";

  // Output line formatting arguments
  public static final String ENCLOSED_BY = "enclosedBy";
  public static final String ESCAPED_BY = "escapedBy";
  public static final String FIELDS_TERMINATED_BY = "fieldsTerminatedBy";
  public static final String LINES_TERMINATED_BY = "linesTerminatedBy";
  public static final String OPTIONALLY_ENCLOSED_BY = "optionallyEnclosedBy";
  public static final String MYSQL_DELIMITERS = "mysqlDelimiters";

  // Input parsing arguments
  public static final String INPUT_ENCLOSED_BY = "inputEnclosedBy";
  public static final String INPUT_ESCAPED_BY = "inputEscapedBy";
  public static final String INPUT_FIELDS_TERMINATED_BY = "inputFieldsTerminatedBy";
  public static final String INPUT_LINES_TERMINATED_BY = "inputLinesTerminatedBy";
  public static final String INPUT_OPTIONALLY_ENCLOSED_BY = "inputOptionallyEnclosedBy";

  // Code generation arguments
  public static final String BIN_DIR = "binDir";
  public static final String CLASS_NAME = "className";
  public static final String JAR_FILE = "jarFile";
  public static final String OUTDIR = "outdir";
  public static final String PACKAGE_NAME = "packageName";
  public static final String MAP_COLUMN_JAVA = "mapColumnJava";

  // Shared Input/Export options
  public static final String TABLE = "table";
  public static final String NUM_MAPPERS = "numMappers";
  public static final String COMMAND_LINE = "commandLine";
  public static final String MODE = "mode";

  public static final String HADOOP_MAPRED_HOME = "hadoopMapredHome";
  public static final String PASSWORD_ALIAS = "passwordAlias";
  public static final String PASSWORD_FILE = "passwordFile";

  public static final String RELAXED_ISOLATION = "relaxedIsolation";
  public static final String SKIP_DIST_CACHE = "skipDistCache";
  public static final String MAPREDUCE_JOB_NAME = "mapreduceJobName";
  public static final String VALIDATE = "validate";
  public static final String VALIDATION_FAILURE_HANDLER = "validationFailureHandler";
  public static final String VALIDATION_THRESHOLD = "validationThreshold";
  public static final String VALIDATOR = "validator";

  public static final String HCATALOG_DATABASE = "hcatalogDatabase";
  public static final String HCATALOG_HOME = "hcatalogHome";
  public static final String HCATALOG_PARTITION_KEYS = "hcatalogPartitionKeys";
  public static final String HCATALOG_PARTITION_VALUES = "hcatalogPartitionValues";
  public static final String HCATALOG_TABLE = "hcatalogTable";

  public static final String HIVE_HOME = "hiveHome";
  public static final String HIVE_PARTITION_KEY = "hivePartitionKey";
  public static final String HIVE_PARTITION_VALUE = "hivePartitionValue";
  public static final String MAP_COLUMN_HIVE = "mapColumnHive";

  public static final String INPUT_NULL_STRING = "inputNullString";
  public static final String INPUT_NULL_NON_STRING = "inputNullNonString";

  public static final String NULL_STRING = "nullString";
  public static final String NULL_NON_STRING = "nullNonString";

  public static final String FILES = "files";
  public static final String LIBJARS = "libjars";
  public static final String ARCHIVES = "archives";

  private String database;
  private String schema;

  // Properties to support toggling between quick setup and advanced mode in the UI. These should never be saved.
  private transient String connectFromAdvanced;
  private transient String usernameFromAdvanced;
  private transient String passwordFromAdvanced;

  @CommandLineArgument( name = "hadoop-mapred-home" )
  private String hadoopMapredHome;
  @CommandLineArgument( name = "password-alias" )
  private String passwordAlias;
  @CommandLineArgument( name = "password-file" )
  private String passwordFile;

  @CommandLineArgument( name = "relaxed-isolation", flag = true )
  private String relaxedIsolation;
  @CommandLineArgument( name = "skip-dist-cache", flag = true )
  private String skipDistCache;

  @CommandLineArgument( name = "mapreduce-job-name" )
  private String mapreduceJobName;

  @CommandLineArgument( name = "validate", flag = true )
  private String validate;
  @CommandLineArgument( name = "validation-failurehandler" )
  private String validationFailureHandler;
  @CommandLineArgument( name = "validation-threshold" )
  private String validationThreshold;
  @CommandLineArgument( name = "validator" )
  private String validator;

  @CommandLineArgument( name = "hcatalog-database" )
  private String hcatalogDatabase;
  @CommandLineArgument( name = "hcatalog-home" )
  private String hcatalogHome;
  @CommandLineArgument( name = "hcatalog-partition-keys" )
  private String hcatalogPartitionKeys;
  @CommandLineArgument( name = "hcatalog-partition-values" )
  private String hcatalogPartitionValues;
  @CommandLineArgument( name = "hcatalog-table" )
  private String hcatalogTable;

  @CommandLineArgument( name = "hive-home" )
  private String hiveHome;
  @CommandLineArgument( name = "hive-partition-key" )
  private String hivePartitionKey;
  @CommandLineArgument( name = "hive-partition-value" )
  private String hivePartitionValue;
  @CommandLineArgument( name = "map-column-hive" )
  private String mapColumnHive;

  @CommandLineArgument( name = "input-null-string" )
  private String inputNullString;
  @CommandLineArgument( name = "input-null-non-string" )
  private String inputNullNonString;
  @CommandLineArgument( name = "null-string" )
  private String nullString;
  @CommandLineArgument( name = "null-non-string" )
  private String nullNonString;

  @CommandLineArgument( name = "files", order = 50, prefix = "-" )
  private String files;
  @CommandLineArgument( name = "libjars", order = 50, prefix = "-" )
  private String libjars;
  @CommandLineArgument( name = "archives", order = 50, prefix = "-" )
  private String archives;

// Represents the last visible state of the UI and the execution mode.
  private String mode;

  // Common arguments
  @CommandLineArgument( name = CONNECT )
  private String connect;

  @CommandLineArgument( name = "connection-manager" )
  private String connectionManager;
  @CommandLineArgument( name = DRIVER )
  private String driver;
  @CommandLineArgument( name = USERNAME )
  private String username;
  @CommandLineArgument( name = PASSWORD )
  @Password
  private String password;
  @CommandLineArgument( name = VERBOSE, flag = true )
  private String verbose;
  @CommandLineArgument( name = "connection-param-file" )
  private String connectionParamFile;
  @CommandLineArgument( name = "hadoop-home" )
  private String hadoopHome;
  // Output line formatting arguments
  @CommandLineArgument( name = "enclosed-by" )
  private String enclosedBy;

  @CommandLineArgument( name = "escaped-by" )
  private String escapedBy;
  @CommandLineArgument( name = "fields-terminated-by" )
  private String fieldsTerminatedBy;
  @CommandLineArgument( name = "lines-terminated-by" )
  private String linesTerminatedBy;
  @CommandLineArgument( name = "optionally-enclosed-by" )
  private String optionallyEnclosedBy;
  @CommandLineArgument( name = "mysql-delimiters", flag = true )
  private String mysqlDelimiters;
  // Input parsing arguments
  @CommandLineArgument( name = "input-enclosed-by" )
  private String inputEnclosedBy;

  @CommandLineArgument( name = "input-escaped-by" )
  private String inputEscapedBy;
  @CommandLineArgument( name = "input-fields-terminated-by" )
  private String inputFieldsTerminatedBy;
  @CommandLineArgument( name = "input-lines-terminated-by" )
  private String inputLinesTerminatedBy;
  @CommandLineArgument( name = "input-optionally-enclosed-by" )
  private String inputOptionallyEnclosedBy;
  // Code generation arguments
  @CommandLineArgument( name = "bindir" )
  private String binDir;

  @CommandLineArgument( name = "class-name" )
  private String className;
  @CommandLineArgument( name = "jar-file" )
  private String jarFile;
  @CommandLineArgument( name = OUTDIR )
  private String outdir;
  @CommandLineArgument( name = "package-name" )
  private String packageName;
  @CommandLineArgument( name = "map-column-java" )
  private String mapColumnJava;

  // Shared Input/Export options
  @CommandLineArgument( name = TABLE )
  private String table;
  @CommandLineArgument( name = "num-mappers" )
  private String numMappers;
  private String commandLine;

  private String clusterName;

  private transient NamedCluster namedCluster;

  private AbstractModelList<PropertyEntry> customArguments;

  /**
   * @return all known arguments for this config object. Some arguments may be synthetic and represent properties
   *         directly set on this config object for the purpose of showing them in the list view of the UI.
   */
  public AbstractModelList<ArgumentWrapper> getAdvancedArgumentsList() {
    final AbstractModelList<ArgumentWrapper> items = new AbstractModelList<ArgumentWrapper>();

    items.addAll( SqoopUtils.findAllArguments( this ) );

    try {
      items.add( new ArgumentWrapper( NAMENODE_HOST, BaseMessages.getString( getClass(), "NamenodeHost.Label" ), false,
          "", 0,
          this, getClass().getMethod( "getNamenodeHost" ), getClass().getMethod( "setNamenodeHost", String.class ) ) );
      items.add( new ArgumentWrapper( NAMENODE_PORT, BaseMessages.getString( getClass(), "NamenodePort.Label" ), false,
          "", 0,
          this, getClass().getMethod( "getNamenodePort" ), getClass().getMethod( "setNamenodePort", String.class ) ) );
      items.add( new ArgumentWrapper( JOBTRACKER_HOST, BaseMessages.getString( getClass(), "JobtrackerHost.Label" ),
          false, "", 0, this,
          getClass().getMethod( "getJobtrackerHost" ), getClass().getMethod( "setJobtrackerHost", String.class ) ) );
      items.add( new ArgumentWrapper( JOBTRACKER_PORT, BaseMessages.getString( getClass(), "JobtrackerPort.Label" ),
          false, "", 0, this,
          getClass().getMethod( "getJobtrackerPort" ), getClass().getMethod( "setJobtrackerPort", String.class ) ) );
      items.add( new ArgumentWrapper( BLOCKING_EXECUTION, BaseMessages
          .getString( getClass(), "BlockingExecution.Label" ),
          false, "", 0, this,
          getClass().getMethod( "getBlockingExecution" ),
          getClass().getMethod( "setBlockingExecution", String.class ) ) );
      items.add( new ArgumentWrapper( BLOCKING_POLLING_INTERVAL, BaseMessages.getString( getClass(),
          "BlockingPollingInterval.Label" ), false, "", 0, this, getClass().getMethod( "getBlockingPollingInterval" ),
          getClass().getMethod( "setBlockingPollingInterval", String.class ) ) );
    } catch ( NoSuchMethodException ex ) {
      throw new RuntimeException( ex );
    }
    return items;
  }

  public NamedCluster getNamedCluster() {
    if ( namedCluster == null ) {
      namedCluster = createClusterTemplate();
    }
    return namedCluster;
  }

  public void setNamedCluster( NamedCluster namedCluster ) {
    this.namedCluster = createClusterTemplate();
    if ( namedCluster != null ) {
      setClusterName( namedCluster.getName() );
      this.namedCluster.replaceMeta( namedCluster );
    }
  }

  protected abstract NamedCluster createClusterTemplate();

  @Override
  public SqoopConfig clone() {
    return (SqoopConfig) super.clone();
  }

  /**
   * Silently set the following properties: {@code database, connect, username, password}.
   *
   * @param database
   *          Database name
   * @param connect
   *          Connection string (JDBC connection URL)
   * @param username
   *          Username
   * @param password
   *          Password
   */
  public void setConnectionInfo( String database, String connect, String username, String password ) {
    this.database = database;
    this.connect = connect;
    this.username = username;
    this.password = password;
  }

  /**
   * Copy connection information from temporary "advanced" fields into annotated argument fields.
   */
  public void copyConnectionInfoFromAdvanced() {
    database = null;
    connect = getConnectFromAdvanced();
    username = getUsernameFromAdvanced();
    password = getPasswordFromAdvanced();
  }

  /**
   * Copy the current connection information into the "advanced" fields. These are temporary session properties used to
   * aid the user during configuration via UI.
   */
  public void copyConnectionInfoToAdvanced() {
    setConnectFromAdvanced( getConnect() );
    setUsernameFromAdvanced( getUsername() );
    setPasswordFromAdvanced( getPassword() );
  }

  // All getters/setters below this line

  public String getDatabase() {
    return database;
  }

  public void setDatabase( String database ) {
    this.database = propertyChange( DATABASE, this.database, database );
  }

  protected String propertyChange( String propertyName, String oldValue, String newValue ) {
    pcs.firePropertyChange( propertyName, oldValue, newValue );
    return newValue;
  }

  public String getSchema() {
    return schema;
  }

  public void setSchema( String schema ) {
    this.schema = propertyChange( SCHEMA, this.schema, schema );
  }

  public String getConnect() {
    return connect;
  }

  public void setConnect( String connect ) {
    this.connect = propertyChange( CONNECT, this.connect, connect );
  }

  public String getUsername() {
    return username;
  }

  public void setUsername( String username ) {
    this.username = propertyChange( USERNAME, this.username, username );
  }

  public String getPassword() {
    return password;
  }

  public void setPassword( String password ) {
    this.password = propertyChange( PASSWORD, this.password, password );
  }

  public String getConnectFromAdvanced() {
    return connectFromAdvanced;
  }

  public void setConnectFromAdvanced( String connectFromAdvanced ) {
    this.connectFromAdvanced = connectFromAdvanced;
  }

  public String getUsernameFromAdvanced() {
    return usernameFromAdvanced;
  }

  public void setUsernameFromAdvanced( String usernameFromAdvanced ) {
    this.usernameFromAdvanced = usernameFromAdvanced;
  }

  public String getPasswordFromAdvanced() {
    return passwordFromAdvanced;
  }

  public void setPasswordFromAdvanced( String passwordFromAdvanced ) {
    this.passwordFromAdvanced = passwordFromAdvanced;
  }

  public String getConnectionManager() {
    return connectionManager;
  }

  public void setConnectionManager( String connectionManager ) {
    this.connectionManager = propertyChange( CONNECTION_MANAGER, this.connectionManager, connectionManager );
  }

  public String getDriver() {
    return driver;
  }

  public void setDriver( String driver ) {
    this.driver = propertyChange( DRIVER, this.driver, driver );
  }

  public String getVerbose() {
    return verbose;
  }

  public void setVerbose( String verbose ) {
    this.verbose = propertyChange( VERBOSE, this.verbose, verbose );
  }

  public String getConnectionParamFile() {
    return connectionParamFile;
  }

  public void setConnectionParamFile( String connectionParamFile ) {
    this.connectionParamFile = propertyChange( CONNECTION_PARAM_FILE, this.connectionParamFile, connectionParamFile );
  }

  public String getHadoopHome() {
    return hadoopHome;
  }

  public void setHadoopHome( String hadoopHome ) {
    this.hadoopHome = propertyChange( HADOOP_HOME, this.hadoopHome, hadoopHome );
  }

  public String getEnclosedBy() {
    return enclosedBy;
  }

  public void setEnclosedBy( String enclosedBy ) {
    this.enclosedBy = propertyChange( ENCLOSED_BY, this.enclosedBy, enclosedBy );
  }

  public String getEscapedBy() {
    return escapedBy;
  }

  public void setEscapedBy( String escapedBy ) {
    this.escapedBy = propertyChange( ESCAPED_BY, this.escapedBy, escapedBy );
  }

  public String getFieldsTerminatedBy() {
    return fieldsTerminatedBy;
  }

  public void setFieldsTerminatedBy( String fieldsTerminatedBy ) {
    this.fieldsTerminatedBy = propertyChange( FIELDS_TERMINATED_BY, this.fieldsTerminatedBy, fieldsTerminatedBy );
  }

  public String getLinesTerminatedBy() {
    return linesTerminatedBy;
  }

  public void setLinesTerminatedBy( String linesTerminatedBy ) {
    this.linesTerminatedBy = propertyChange( LINES_TERMINATED_BY, this.linesTerminatedBy, linesTerminatedBy );
  }

  public String getOptionallyEnclosedBy() {
    return optionallyEnclosedBy;
  }

  public void setOptionallyEnclosedBy( String optionallyEnclosedBy ) {
    this.optionallyEnclosedBy = propertyChange( OPTIONALLY_ENCLOSED_BY, this.optionallyEnclosedBy, optionallyEnclosedBy );
  }

  public String getMysqlDelimiters() {
    return mysqlDelimiters;
  }

  public void setMysqlDelimiters( String mysqlDelimiters ) {
    this.mysqlDelimiters = propertyChange( MYSQL_DELIMITERS, this.mysqlDelimiters, mysqlDelimiters );
  }

  public String getInputEnclosedBy() {
    return inputEnclosedBy;
  }

  public void setInputEnclosedBy( String inputEnclosedBy ) {
    this.inputEnclosedBy = propertyChange( INPUT_ENCLOSED_BY, this.inputEnclosedBy, inputEnclosedBy );
  }

  public String getInputEscapedBy() {
    return inputEscapedBy;
  }

  public void setInputEscapedBy( String inputEscapedBy ) {
    this.inputEscapedBy = propertyChange( INPUT_ESCAPED_BY, this.inputEscapedBy, inputEscapedBy );
  }

  public String getInputFieldsTerminatedBy() {
    return inputFieldsTerminatedBy;
  }

  public void setInputFieldsTerminatedBy( String inputFieldsTerminatedBy ) {
    this.inputFieldsTerminatedBy = propertyChange( INPUT_FIELDS_TERMINATED_BY, this.inputFieldsTerminatedBy, inputFieldsTerminatedBy );
  }

  public String getInputLinesTerminatedBy() {
    return inputLinesTerminatedBy;
  }

  public void setInputLinesTerminatedBy( String inputLinesTerminatedBy ) {
    this.inputLinesTerminatedBy = propertyChange( INPUT_LINES_TERMINATED_BY, this.inputLinesTerminatedBy, inputLinesTerminatedBy );
  }

  public String getInputOptionallyEnclosedBy() {
    return inputOptionallyEnclosedBy;
  }

  public void setInputOptionallyEnclosedBy( String inputOptionallyEnclosedBy ) {
    this.inputOptionallyEnclosedBy = propertyChange( INPUT_OPTIONALLY_ENCLOSED_BY, this.inputOptionallyEnclosedBy, inputOptionallyEnclosedBy );
  }

  public String getBinDir() {
    return binDir;
  }

  public void setBinDir( String binDir ) {
    this.binDir = propertyChange( BIN_DIR, this.binDir, binDir );
  }

  public String getClassName() {
    return className;
  }

  public void setClassName( String className ) {
    this.className = propertyChange( CLASS_NAME, this.className, className );
  }

  public String getJarFile() {
    return jarFile;
  }

  public void setJarFile( String jarFile ) {
    this.jarFile = propertyChange( JAR_FILE, this.jarFile, jarFile );
  }

  public String getOutdir() {
    return outdir;
  }

  public void setOutdir( String outdir ) {
    this.outdir = propertyChange( OUTDIR, this.outdir, outdir );
  }

  public String getPackageName() {
    return packageName;
  }

  public void setPackageName( String packageName ) {
    this.packageName = propertyChange( PACKAGE_NAME, this.packageName, packageName );
  }

  public String getMapColumnJava() {
    return mapColumnJava;
  }

  public void setMapColumnJava( String mapColumnJava ) {
    this.mapColumnJava = propertyChange( MAP_COLUMN_JAVA, this.mapColumnJava, mapColumnJava );
  }

  public String getTable() {
    return table;
  }

  public void setTable( String table ) {
    this.table = propertyChange( TABLE, this.table, table );
  }

  public String getNumMappers() {
    return numMappers;
  }

  public void setNumMappers( String numMappers ) {
    this.numMappers = propertyChange( NUM_MAPPERS, this.numMappers, numMappers );
  }

  public String getCommandLine() {
    return commandLine;
  }

  public void setCommandLine( String commandLine ) {
    this.commandLine = propertyChange( COMMAND_LINE, this.commandLine, commandLine );
  }

  public String getMode() {
    return mode;
  }

  public JobEntryMode getModeAsEnum() {
    try {
      return JobEntryMode.valueOf( getMode() );
    } catch ( Exception ex ) {
      // Not a valid ui mode, return the default
      return JobEntryMode.QUICK_SETUP;
    }
  }

  /**
   * Sets the mode based on the enum value
   *
   * @param mode
   */
  public void setMode( JobEntryMode mode ) {
    setMode( mode.name() );
  }

  public void setMode( String mode ) {
    this.mode = propertyChange( MODE, this.mode, mode );
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName( String clusterName ) {
    this.clusterName = propertyChange( "clusterName", this.clusterName, clusterName );
  }

  public String getNamenodeHost() {
    return getNamedCluster().getHdfsHost();
  }

  public void setNamenodeHost( String namenodeHost ) {
    getNamedCluster().setHdfsHost( propertyChange( NAMENODE_HOST, getNamenodeHost(), namenodeHost ) );
  }

  public String getNamenodePort() {
    return getNamedCluster().getHdfsPort();
  }

  public void setNamenodePort( String namenodePort ) {
    getNamedCluster().setHdfsPort( propertyChange( NAMENODE_PORT, getNamenodePort(), namenodePort ) );
  }

  public String getJobtrackerHost() {
    return getNamedCluster().getJobTrackerHost();
  }

  public void setJobtrackerHost( String jobtrackerHost ) {
    getNamedCluster().setJobTrackerHost( propertyChange( JOBTRACKER_HOST, getJobtrackerHost(), jobtrackerHost ) );
  }

  public String getJobtrackerPort() {
    return getNamedCluster().getJobTrackerPort();
  }

  public void setJobtrackerPort( String jobtrackerPort ) {
    getNamedCluster().setJobTrackerPort( propertyChange( JOBTRACKER_PORT, getJobtrackerPort(), jobtrackerPort ) );
  }

  public String getHadoopMapredHome() {
    return hadoopMapredHome;
  }

  public void setHadoopMapredHome( String hadoopMapredHome ) {
    this.hadoopMapredHome = propertyChange( HADOOP_MAPRED_HOME, this.hadoopMapredHome, hadoopMapredHome );
  }

  public String getPasswordAlias() {
    return passwordAlias;
  }

  public void setPasswordAlias( String passwordAlias ) {
    this.passwordAlias = propertyChange( PASSWORD_ALIAS, this.passwordAlias, passwordAlias );
  }

  public String getPasswordFile() {
    return passwordFile;
  }

  public void setPasswordFile( String passwordFile ) {
    this.passwordFile = propertyChange( PASSWORD_FILE, this.passwordFile, passwordFile );
  }

  public String getRelaxedIsolation() {
    return relaxedIsolation;
  }

  public void setRelaxedIsolation( String relaxedIsolation ) {
    this.relaxedIsolation = propertyChange( RELAXED_ISOLATION, this.relaxedIsolation, relaxedIsolation );
  }

  public String getSkipDistCache() {
    return skipDistCache;
  }

  public void setSkipDistCache( String skipDistCache ) {
    this.skipDistCache = propertyChange( SKIP_DIST_CACHE, this.skipDistCache, skipDistCache );
  }

  public String getMapreduceJobName() {
    return mapreduceJobName;
  }

  public void setMapreduceJobName( String mapreduceJobName ) {
    this.mapreduceJobName = propertyChange( MAPREDUCE_JOB_NAME, this.mapreduceJobName, mapreduceJobName );
  }

  public String getValidate() {
    return validate;
  }

  public void setValidate( String validate ) {
    this.validate = propertyChange( VALIDATE, this.validate, validate );
  }

  public String getValidationFailureHandler() {
    return validationFailureHandler;
  }

  public void setValidationFailureHandler( String validationFailureHandler ) {
    this.validationFailureHandler = propertyChange( VALIDATION_FAILURE_HANDLER, this.validationFailureHandler, validationFailureHandler );
  }

  public String getValidationThreshold() {
    return validationThreshold;
  }

  public void setValidationThreshold( String validationThreshold ) {
    this.validationThreshold = propertyChange( VALIDATION_THRESHOLD, this.validationThreshold, validationThreshold );
  }

  public String getValidator() {
    return validator;
  }

  public void setValidator( String validator ) {
    this.validator = propertyChange( VALIDATOR, this.validator, validator );
  }

  public String getHcatalogDatabase() {
    return hcatalogDatabase;
  }

  public void setHcatalogDatabase( String hcatalogDatabase ) {
    this.hcatalogDatabase = propertyChange( HCATALOG_DATABASE, this.hcatalogDatabase, hcatalogDatabase );
  }

  public String getHcatalogHome() {
    return hcatalogHome;
  }

  public void setHcatalogHome( String hcatalogHome ) {
    this.hcatalogHome = propertyChange( HCATALOG_HOME, this.hcatalogHome, hcatalogHome );
  }

  public String getHcatalogPartitionKeys() {
    return hcatalogPartitionKeys;
  }

  public void setHcatalogPartitionKeys( String hcatalogPartitionKeys ) {
    this.hcatalogPartitionKeys = propertyChange( HCATALOG_PARTITION_KEYS, this.hcatalogPartitionKeys, hcatalogPartitionKeys );
  }

  public String getHcatalogPartitionValues() {
    return hcatalogPartitionValues;
  }

  public void setHcatalogPartitionValues( String hcatalogPartitionValues ) {
    this.hcatalogPartitionValues = propertyChange( HCATALOG_PARTITION_VALUES, this.hcatalogPartitionValues, hcatalogPartitionValues );
  }

  public String getHcatalogTable() {
    return hcatalogTable;
  }

  public void setHcatalogTable( String hcatalogTable ) {
    this.hcatalogTable = propertyChange( HCATALOG_TABLE, this.hcatalogTable, hcatalogTable );
  }

  public String getHiveHome() {
    return hiveHome;
  }

  public void setHiveHome( String hiveHome ) {
    this.hiveHome = propertyChange( HIVE_HOME, this.hiveHome, hiveHome );
  }

  public String getHivePartitionKey() {
    return hivePartitionKey;
  }

  public void setHivePartitionKey( String hivePartitionKey ) {
    this.hivePartitionKey = propertyChange( HIVE_PARTITION_KEY, this.hivePartitionKey, hivePartitionKey );
  }

  public String getHivePartitionValue() {
    return hivePartitionValue;
  }

  public void setHivePartitionValue( String hivePartitionValue ) {
    this.hivePartitionValue = propertyChange( HIVE_PARTITION_VALUE, this.hivePartitionValue, hivePartitionValue );
  }

  public String getMapColumnHive() {
    return mapColumnHive;
  }

  public void setMapColumnHive( String mapColumnHive ) {
    this.mapColumnHive = propertyChange( MAP_COLUMN_HIVE, this.mapColumnHive, mapColumnHive );
  }
  public String getInputNullString() {
    return inputNullString;
  }

  public void setInputNullString( String inputNullString ) {
    this.inputNullString = propertyChange( INPUT_NULL_STRING, this.inputNullString, inputNullString );
  }

  public String getInputNullNonString() {
    return inputNullNonString;
  }

  public void setInputNullNonString( String inputNullNonString ) {
    this.inputNullNonString = propertyChange( INPUT_NULL_NON_STRING, this.inputNullNonString, inputNullNonString );
  }
  public String getNullString() {
    return nullString;
  }

  public void setNullString( String nullString ) {
    this.nullString = propertyChange( NULL_STRING, this.nullString, nullString );
  }

  public String getNullNonString() {
    return nullNonString;
  }

  public void setNullNonString( String nullNonString ) {
    this.nullNonString = propertyChange( NULL_NON_STRING, this.nullNonString, nullNonString );
  }

  public String getFiles() {
    return files;
  }

  public void setFiles( String files ) {
    this.files = propertyChange( FILES, this.files, files );
  }

  public String getLibjars() {
    return libjars;
  }

  public void setLibjars( String libjars ) {
    this.libjars = propertyChange( LIBJARS, this.libjars, libjars );
  }

  public String getArchives() {
    return archives;
  }

  public void setArchives( String archives ) {
    this.archives = propertyChange( ARCHIVES, this.archives, archives );
  }

  public AbstractModelList<PropertyEntry> getCustomArguments() {
    if ( customArguments == null ) {
      customArguments = new AbstractModelList<>();
    }
    return customArguments;
  }

  public void setCustomArguments( AbstractModelList<PropertyEntry> customArguments ) {
    this.customArguments = customArguments;
  }

  public void loadClusterConfig( Repository rep, ObjectId id ) throws KettleException {
    setNamedCluster( null );
    setNamenodeHost( rep.getJobEntryAttributeString( id, NAMENODE_HOST ) );
    setNamenodePort( rep.getJobEntryAttributeString( id, NAMENODE_PORT ) );
    setJobtrackerHost( rep.getJobEntryAttributeString( id, JOBTRACKER_HOST ) );
    setJobtrackerPort( rep.getJobEntryAttributeString( id, JOBTRACKER_PORT ) );
  }

  public void loadClusterConfig( Node entrynode ) {
    setNamedCluster( null );
    setNamenodeHost( XMLHandler.getTagValue( entrynode, NAMENODE_HOST ) );
    setNamenodePort( XMLHandler.getTagValue( entrynode, NAMENODE_PORT ) );
    setJobtrackerHost( XMLHandler.getTagValue( entrynode, JOBTRACKER_HOST ) );
    setJobtrackerPort( XMLHandler.getTagValue( entrynode, JOBTRACKER_PORT ) );
  }

  public String getClusterXML() {
    StringBuilder builder = new StringBuilder();
    for ( Map.Entry<String, String> entry : namedClusterProperties( getNamedCluster() ).entrySet() ) {
      builder.append( XMLHandler.addTagValue( entry.getKey(), entry.getValue() ) );
    }
    return builder.toString();
  }

  public void saveClusterConfig( Repository rep, ObjectId id_job, JobEntryInterface jobEntry ) throws KettleException {
    ObjectId objectId = jobEntry.getObjectId();
    for ( Map.Entry<String, String> entry : namedClusterProperties( getNamedCluster() ).entrySet() ) {
      rep.saveJobEntryAttribute( id_job, objectId, entry.getKey(), entry.getValue() );
    }
  }

  public boolean isAdvancedClusterConfigSet() {
    return Strings.isNullOrEmpty( getClusterName() ) && ncPropertiesNotNullOrEmpty( getNamedCluster() );
  }

  private static Map<String, String> namedClusterProperties( NamedCluster namedCluster ) {
    return ImmutableMap.of(
      NAMENODE_HOST, Strings.nullToEmpty( namedCluster.getHdfsHost() ),
      NAMENODE_PORT, Strings.nullToEmpty( namedCluster.getHdfsPort() ),
      JOBTRACKER_HOST, Strings.nullToEmpty( namedCluster.getJobTrackerHost() ),
      JOBTRACKER_PORT, Strings.nullToEmpty( namedCluster.getJobTrackerPort() )
    );
  }

  @VisibleForTesting
    boolean ncPropertiesNotNullOrEmpty( NamedCluster nc ) {
    return !Strings.isNullOrEmpty( nc.getHdfsHost() ) || !Strings.isNullOrEmpty( nc.getHdfsPort() ) || !Strings.isNullOrEmpty( nc.getJobTrackerHost() ) || !Strings.isNullOrEmpty( nc
        .getJobTrackerPort() );
  }

}
