/*******************************************************************************
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

package org.pentaho.big.data.kettle.plugins.hbase.input;

import com.google.common.annotations.VisibleForTesting;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterServiceLocator;
import org.pentaho.hadoop.shim.api.cluster.ClusterInitializationException;
import org.pentaho.big.data.kettle.plugins.hbase.FilterDefinition;
import org.pentaho.big.data.kettle.plugins.hbase.MappingDefinition;
import org.pentaho.big.data.kettle.plugins.hbase.NamedClusterLoadSaveUtil;
import org.pentaho.big.data.kettle.plugins.hbase.ServiceStatus;
import org.pentaho.big.data.kettle.plugins.hbase.mapping.MappingAdmin;
import org.pentaho.big.data.kettle.plugins.hbase.mapping.MappingUtils;
import org.pentaho.big.data.kettle.plugins.hbase.meta.AELHBaseMappingImpl;
import org.pentaho.big.data.kettle.plugins.hbase.meta.AELHBaseValueMetaImpl;
import org.pentaho.hadoop.shim.api.hbase.ByteConversionUtil;
import org.pentaho.hadoop.shim.api.hbase.HBaseConnection;
import org.pentaho.hadoop.shim.api.hbase.HBaseService;
import org.pentaho.hadoop.shim.api.hbase.mapping.ColumnFilter;
import org.pentaho.hadoop.shim.api.hbase.mapping.ColumnFilterFactory;
import org.pentaho.hadoop.shim.api.hbase.mapping.Mapping;
import org.pentaho.hadoop.shim.api.hbase.mapping.MappingFactory;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterface;
import org.pentaho.hadoop.shim.api.hbase.meta.HBaseValueMetaInterfaceFactory;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionDeep;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.osgi.api.MetastoreLocatorOsgi;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class providing an input step for reading data from an HBase table according to meta data mapping info stored in a
 * separate HBase table called "pentaho_mappings". See org.pentaho.hbase.mapping.Mapping for details on the meta data
 * format.
 */
@Step( id = "HBaseInput", image = "HB.svg", name = "HBaseInput.Name", description = "HBaseInput.Description",
    categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.BigData",
    documentationUrl = "Products/HBase_Input",
    i18nPackageName = "org.pentaho.di.trans.steps.hbaseinput" )
@InjectionSupported( localizationPrefix = "HBaseInput.Injection.", groups = {"OUTPUT_FIELDS", "MAPPING", "FILTER"} )
public class HBaseInputMeta extends BaseStepMeta implements StepMetaInterface {

  protected static Class<?> PKG = HBaseInputMeta.class;

  private final NamedClusterLoadSaveUtil namedClusterLoadSaveUtil;
  private final NamedClusterService namedClusterService;
  private final NamedClusterServiceLocator namedClusterServiceLocator;
  private final RuntimeTestActionService runtimeTestActionService;
  private MetastoreLocatorOsgi metaStoreService;
  private final RuntimeTester runtimeTester;

  protected NamedCluster namedCluster;

  /**
   * path/url to hbase-site.xml
   */
  @Injection( name = "HBASE_SITE_XML_URL" )
  protected String m_coreConfigURL;

  /**
   * path/url to hbase-default.xml
   */
  @Injection( name = "HBASE_DEFAULT_XML_URL" )
  protected String m_defaultConfigURL;

  /**
   * the name of the HBase table to read from
   */
  @Injection( name = "SOURCE_TABLE_NAME" )
  protected String m_sourceTableName;

  /**
   * the name of the mapping for columns/types for the source table
   */
  @Injection( name = "SOURCE_MAPPING_NAME" )
  protected String m_sourceMappingName;

  /**
   * Start key value for range scans
   */
  @Injection( name = "START_KEY_VALUE" )
  protected String m_keyStart;

  /**
   * Stop key value for range scans
   */
  @Injection( name = "STOP_KEY_VALUE" )
  protected String m_keyStop;

  /**
   * Scanner caching
   */
  @Injection( name = "SCANNER_ROW_CACHE_SIZE" )
  protected String m_scannerCacheSize;

  protected transient Mapping m_cachedMapping;

  /**
   * The selected fields to output. If null, then all fields from the mapping are output
   */
  protected List<HBaseValueMetaInterface> m_outputFields;

  @InjectionDeep
  protected List<OutputFieldDefinition> outputFieldsDefinition;

  /**
   * The configured column filters. If null, then no filters are applied to the result set
   */
  protected List<ColumnFilter> m_filters;

  @InjectionDeep
  protected List<FilterDefinition> filtersDefinition;

  /**
   * If true, then any matching filter will cause the row to be output, otherwise all filters have to return true before
   * the row is output
   */
  @Injection( name = "MATCH_ANY_FILTER" )
  protected boolean m_matchAnyFilter;

  /**
   * The mapping to use if we are not loading one dynamically at runtime from HBase itself
   */
  protected Mapping m_mapping;

  @InjectionDeep
  protected MappingDefinition mappingDefinition;

  private ServiceStatus serviceStatus = ServiceStatus.OK;

  public HBaseInputMeta( NamedClusterService namedClusterService,
                         NamedClusterServiceLocator namedClusterServiceLocator,
                         RuntimeTestActionService runtimeTestActionService, RuntimeTester runtimeTester, MetastoreLocatorOsgi metaStore ) {
    this.namedClusterService = namedClusterService;
    this.namedClusterServiceLocator = namedClusterServiceLocator;
    this.runtimeTestActionService = runtimeTestActionService;
    this.runtimeTester = runtimeTester;
    namedClusterLoadSaveUtil = new NamedClusterLoadSaveUtil();
    this.metaStoreService = metaStore;
  }

  /**
   * Set the mapping to use for decoding the row
   *
   * @param m the mapping to use
   */
  public void setMapping( Mapping m ) {
    m_mapping = m;
  }

  /**
   * Get the mapping to use for decoding the row
   *
   * @return the mapping to use
   */
  public Mapping getMapping() {
    return m_mapping;
  }

  /**
   * Set the URL to the hbase-site.xml. Either this OR the zookeeper host list can be used to establish a connection.
   *
   * @param coreConfig
   */
  public void setCoreConfigURL( String coreConfig ) {
    m_coreConfigURL = coreConfig;
    m_cachedMapping = null;
  }

  /**
   * Get the URL to the hbase-site.xml file.
   *
   * @return the URL to the hbase-site.xml file or null if not set.
   */
  public String getCoreConfigURL() {
    return m_coreConfigURL;
  }

  /**
   * Set the URL to the hbase-default.xml file. This can be optionally supplied in conjuction with hbase-site.xml. If
   * not supplied, then the default hbase-default.xml included in the main hbase jar file is used.
   *
   * @param defaultConfig URL to the hbase-default.xml file.
   */
  public void setDefaulConfigURL( String defaultConfig ) {
    m_defaultConfigURL = defaultConfig;
    m_cachedMapping = null;
  }

  /**
   * Get the URL to hbase-default.xml
   *
   * @return the URL to hbase-default.xml or null if not set.
   */
  public String getDefaultConfigURL() {
    return m_defaultConfigURL;
  }

  public void setSourceTableName( String sourceTable ) {
    m_sourceTableName = sourceTable;
    m_cachedMapping = null;
  }

  /**
   * Get the name of the HBase table to read from.
   *
   * @return the name of the source HBase table.
   */
  public String getSourceTableName() {
    return m_sourceTableName;
  }

  /**
   * Set the name of the mapping to use that defines column names and types for the source table.
   *
   * @param sourceMapping the name of the mapping to use.
   */
  public void setSourceMappingName( String sourceMapping ) {
    m_sourceMappingName = sourceMapping;
    m_cachedMapping = null;
  }

  /**
   * Get the name of the mapping to use for reading and decoding column values for the source table.
   *
   * @return the name of the mapping to use.
   */
  public String getSourceMappingName() {
    return m_sourceMappingName;
  }

  /**
   * Set whether a given row needs to match at least one of the user specified column filters.
   *
   * @param a true if at least one filter needs to match before a given row is returned. If false then *all* filters
   *          must match.
   */
  public void setMatchAnyFilter( boolean a ) {
    m_matchAnyFilter = a;
  }

  /**
   * Get whether a given row needs to match at least one of the user-specified column filters.
   *
   * @return true if a given row needs to match at least one of the user specified column filters. Returns false if
   * *all* column filters need to match
   */
  public boolean getMatchAnyFilter() {
    return m_matchAnyFilter;
  }

  /**
   * Set the starting value (inclusive) of the key for range scans
   *
   * @param start the starting value of the key to use in range scans.
   */
  public void setKeyStartValue( String start ) {
    m_keyStart = start;
  }

  /**
   * Get the starting value of the key to use in range scans
   *
   * @return the starting value of the key
   */
  public String getKeyStartValue() {
    return m_keyStart;
  }

  /**
   * Set the stop value (exclusive) of the key to use in range scans. May be null to indicate scan to the end of the
   * table
   *
   * @param stop the stop value of the key to use in range scans
   */
  public void setKeyStopValue( String stop ) {
    m_keyStop = stop;
  }

  /**
   * Get the stop value of the key to use in range scans
   *
   * @return the stop value of the key
   */
  public String getKeyStopValue() {
    return m_keyStop;
  }

  /**
   * Set the number of rows to cache for scans. Higher values result in improved performance since there will be fewer
   * requests to HBase but at the expense of increased memory consumption.
   *
   * @param s the number of rows to cache for scans.
   */
  public void setScannerCacheSize( String s ) {
    m_scannerCacheSize = s;
  }

  /**
   * The number of rows to cache for scans.
   *
   * @return the number of rows to cache for scans.
   */
  public String getScannerCacheSize() {
    return m_scannerCacheSize;
  }

  /**
   * Set a list of fields to emit from this steo. If not specified, then all fields defined in the mapping for the
   * source table will be emitted.
   *
   * @param fields a list of fields to emit from this step.
   */
  public void setOutputFields( List<HBaseValueMetaInterface> fields ) {
    m_outputFields = fields;
  }

  /**
   * Get the list of fields to emit from this step. May return null, which indicates that *all* fields defined in the
   * mapping for the source table will be emitted.
   *
   * @return the fields that will be output or null (indicating all fields defined in the mapping will be output).
   */
  public List<HBaseValueMetaInterface> getOutputFields() {
    return m_outputFields;
  }

  /**
   * Set a list of column filters to use to refine the query
   *
   * @param list a list of column filters to refine the query
   */
  public void setColumnFilters( List<ColumnFilter> list ) {
    m_filters = list;
  }

  /**
   * Get the list of column filters to use for refining the results of a scan. May return null if no filters are in use.
   *
   * @return a list of columm filters by which to refine the results of a query scan.
   */
  public List<ColumnFilter> getColumnFilters() {
    return m_filters;
  }

  public void setDefault() {
    m_coreConfigURL = null;
    m_defaultConfigURL = null;
    m_cachedMapping = null;
    m_sourceTableName = null;
    m_sourceMappingName = null;
    m_keyStart = null;
    m_keyStop = null;
    namedCluster = namedClusterService.getClusterTemplate();
  }

  private String getIndexValues( HBaseValueMetaInterface vm ) {
    Object[] labels = vm.getIndex();
    StringBuffer vals = new StringBuffer();
    vals.append( "{" );

    for ( int i = 0; i < labels.length; i++ ) {
      if ( i != labels.length - 1 ) {
        vals.append( labels[i].toString().trim() ).append( "," );
      } else {
        vals.append( labels[i].toString().trim() ).append( "}" );
      }
    }
    return vals.toString();
  }

  void applyInjection( VariableSpace space ) throws KettleException {
    if ( namedCluster == null ) {
      throw new KettleException( "Named cluster was not initialized!" );
    }
    try {
      HBaseService hBaseService = getService();
      Mapping tempMapping = null;
      if ( mappingDefinition != null ) {
        tempMapping = getMapping( mappingDefinition, hBaseService );
        setMapping( tempMapping );
      }

      if ( outputFieldsDefinition != null && !outputFieldsDefinition.isEmpty() ) {
        if ( mappingDefinition == null ) {
          if ( !Const.isEmpty( m_sourceMappingName ) ) {
            tempMapping =
                getMappingFromHBase( hBaseService, space, m_sourceTableName, m_sourceMappingName, m_coreConfigURL,
                    m_defaultConfigURL );
          } else {
            tempMapping = m_mapping;
          }
        }
        setOutputFields( createOutputFieldsDefinition( tempMapping, hBaseService ) );
      }

      if ( filtersDefinition != null && !filtersDefinition.isEmpty() ) {
        ColumnFilterFactory columnFilterFactory = hBaseService.getColumnFilterFactory();
        setColumnFilters( createColumnFiltersFromDefinition( columnFilterFactory ) );
      }
    } catch ( Exception e ) {
      throw new KettleException( e );
    }
  }

  @VisibleForTesting
  Mapping getMapping( MappingDefinition mappingDefinition, HBaseService hBaseService ) throws KettleException {
    return MappingUtils.getMapping( mappingDefinition, hBaseService );
  }

  static Mapping getMappingFromHBase( HBaseService hBaseService, VariableSpace space, String tableName,
                                      String mappingName, String coreConfigURL, String defaultConfigURL ) throws KettleException {
    try {
      String siteConfig = "";
      if ( !Const.isEmpty( coreConfigURL ) ) {
        siteConfig = space.environmentSubstitute( coreConfigURL );
      }
      String defaultConfig = "";
      if ( !Const.isEmpty( ( defaultConfigURL ) ) ) {
        defaultConfig = space.environmentSubstitute( defaultConfigURL );
      }
      MappingAdmin mappingAdmin = MappingUtils.getMappingAdmin( hBaseService, space, siteConfig, defaultConfig );
      return mappingAdmin.getMapping( tableName, mappingName );
    } catch ( Exception e ) {
      throw new KettleException( e );
    }
  }

  @VisibleForTesting
  List<HBaseValueMetaInterface> createOutputFieldsDefinition( Mapping mapping, HBaseService hBaseService ) {
    return createOutputFieldsDefinition( outputFieldsDefinition, mapping, hBaseService );
  }

  static List<HBaseValueMetaInterface> createOutputFieldsDefinition(
      List<OutputFieldDefinition> outputFieldsDefinition, Mapping m_mapping, HBaseService hBaseService ) {
    HBaseValueMetaInterfaceFactory valueMetaInterfaceFactory = hBaseService.getHBaseValueMetaInterfaceFactory();
    ByteConversionUtil byteConversionUtil = hBaseService.getByteConversionUtil();

    List<HBaseValueMetaInterface> outputFields = new ArrayList<>();
    Map<String, HBaseValueMetaInterface> columns = m_mapping.getMappedColumns();
    for ( OutputFieldDefinition fieldDefinition : outputFieldsDefinition ) {
      HBaseValueMetaInterface valueMeta =
          valueMetaInterfaceFactory.createHBaseValueMetaInterface( fieldDefinition.getFamily(), fieldDefinition
                  .getColumnName(), fieldDefinition.getAlias(), ValueMeta.getType( fieldDefinition.getHbaseType() ), -1,
              -1 );
      valueMeta.setKey( fieldDefinition.isKey() );
      valueMeta.setConversionMask( fieldDefinition.getFormat() );
      HBaseValueMetaInterface mappedColumn = columns.get( fieldDefinition.getAlias() );
      if ( mappedColumn != null && mappedColumn.getIndex() != null ) {
        Object[] indexVal = mappedColumn.getIndex();
        String indexStrign = byteConversionUtil.objectIndexValuesToString( indexVal );

        Object[] vals = byteConversionUtil.stringIndexListToObjects( indexStrign );
        valueMeta.setIndex( vals );
        valueMeta.setStorageType( ValueMetaInterface.STORAGE_TYPE_INDEXED );
      }
      outputFields.add( valueMeta );
    }
    return outputFields;
  }

  @VisibleForTesting
  List<ColumnFilter> createColumnFiltersFromDefinition( ColumnFilterFactory c ) {
    return createColumnFiltersFromDefinition( filtersDefinition, c );
  }

  static List<ColumnFilter> createColumnFiltersFromDefinition( List<FilterDefinition> filtersDefinition,
                                                               ColumnFilterFactory columnFilterFactory ) {
    List<ColumnFilter> filters = new ArrayList<>();
    for ( FilterDefinition filterDefinition : filtersDefinition ) {
      ColumnFilter columnFilter = columnFilterFactory.createFilter( filterDefinition.getAlias() );
      columnFilter.setFieldType( filterDefinition.getFieldType() );
      columnFilter.setComparisonOperator( filterDefinition.getComparisonType() );
      columnFilter.setConstant( filterDefinition.getConstant() );
      columnFilter.setSignedComparison( filterDefinition.isSignedComparison() );
      columnFilter.setFormat( filterDefinition.getFormat() );
      filters.add( columnFilter );
    }
    return filters;
  }

  @Override
  public String getXML() {
    try {
      applyInjection( new Variables() );
    } catch ( KettleException e ) {
      logError( "Error occurred while injecting metadata. Transformation meta could be incorrect!", e );
    }
    StringBuilder retval = new StringBuilder();

    namedClusterLoadSaveUtil
        .getXml( retval, namedClusterService, namedCluster, repository == null ? null : repository.getMetaStore(), getLog() );

    if ( !Const.isEmpty( m_coreConfigURL ) ) {
      retval.append( "\n    " ).append( XMLHandler.addTagValue( "core_config_url", m_coreConfigURL ) );
    }
    if ( !Const.isEmpty( m_defaultConfigURL ) ) {
      retval.append( "\n    " ).append( XMLHandler.addTagValue( "default_config_url", m_defaultConfigURL ) );
    }
    if ( !Const.isEmpty( m_sourceTableName ) ) {
      retval.append( "\n    " ).append( XMLHandler.addTagValue( "source_table_name", m_sourceTableName ) );
    }
    if ( !Const.isEmpty( m_sourceMappingName ) ) {
      retval.append( "\n    " ).append( XMLHandler.addTagValue( "source_mapping_name", m_sourceMappingName ) );
    }
    if ( !Const.isEmpty( m_keyStart ) ) {
      retval.append( "\n    " ).append( XMLHandler.addTagValue( "key_start", m_keyStart ) );
    }
    if ( !Const.isEmpty( m_keyStop ) ) {
      retval.append( "\n    " ).append( XMLHandler.addTagValue( "key_stop", m_keyStop ) );
    }
    if ( !Const.isEmpty( m_scannerCacheSize ) ) {
      retval.append( "\n    " ).append( XMLHandler.addTagValue( "scanner_cache_size", m_scannerCacheSize ) );
    }

    if ( m_outputFields != null && m_outputFields.size() > 0 ) {
      retval.append( "\n    " ).append( XMLHandler.openTag( "output_fields" ) );

      for ( HBaseValueMetaInterface vm : m_outputFields ) {
        vm.getXml( retval );
      }

      retval.append( "\n    " ).append( XMLHandler.closeTag( "output_fields" ) );
    }

    if ( m_filters != null && m_filters.size() > 0 ) {
      retval.append( "\n    " ).append( XMLHandler.openTag( "column_filters" ) );

      for ( ColumnFilter f : m_filters ) {
        f.appendXML( retval );
      }
      retval.append( "\n    " ).append( XMLHandler.closeTag( "column_filters" ) );
    }

    retval.append( "\n    " ).append( XMLHandler.addTagValue( "match_any_filter", m_matchAnyFilter ) );

    if ( m_mapping != null ) {
      retval.append( m_mapping.getXML() );
    }

    return retval.toString();
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore )
      throws KettleXMLException {
    System.out.println( "loading data" );

    if ( metaStore == null ) {
      metaStore = metaStoreService.getMetastore();
    }

    this.namedCluster =
        namedClusterLoadSaveUtil.loadClusterConfig( namedClusterService, null, repository, metaStore, stepnode, getLog() );

    HBaseService hBaseService = null;
    try {
      hBaseService = getService();
    } catch ( Exception e ) {
      getLog().logError( e.getMessage() );
    }

    m_coreConfigURL = XMLHandler.getTagValue( stepnode, "core_config_url" );
    m_defaultConfigURL = XMLHandler.getTagValue( stepnode, "default_config_url" );
    m_sourceTableName = XMLHandler.getTagValue( stepnode, "source_table_name" );
    m_sourceMappingName = XMLHandler.getTagValue( stepnode, "source_mapping_name" );
    m_keyStart = XMLHandler.getTagValue( stepnode, "key_start" );
    m_keyStop = XMLHandler.getTagValue( stepnode, "key_stop" );
    m_scannerCacheSize = XMLHandler.getTagValue( stepnode, "scanner_cache_size" );
    String m = XMLHandler.getTagValue( stepnode, "match_any_filter" );
    if ( !Const.isEmpty( m ) ) {
      m_matchAnyFilter = m.equalsIgnoreCase( "Y" );
    }

    if ( hBaseService != null ) {
      HBaseValueMetaInterfaceFactory valueMetaInterfaceFactory = hBaseService.getHBaseValueMetaInterfaceFactory();
      m_outputFields = valueMetaInterfaceFactory.createListFromNode( stepnode );

      ColumnFilterFactory columnFilterFactory = hBaseService.getColumnFilterFactory();
      MappingFactory mappingFactory = hBaseService.getMappingFactory();

      Node filters = XMLHandler.getSubNode( stepnode, "column_filters" );
      if ( filters != null && XMLHandler.countNodes( filters, "filter" ) > 0 ) {
        int nrFilters = XMLHandler.countNodes( filters, "filter" );
        m_filters = new ArrayList<ColumnFilter>();

        for ( int i = 0; i < nrFilters; i++ ) {
          Node filterNode = XMLHandler.getSubNodeByNr( filters, "filter", i );
          m_filters.add( columnFilterFactory.createFilter( filterNode ) );
        }
      }

      Mapping tempMapping = mappingFactory.createMapping();
      if ( tempMapping.loadXML( stepnode ) ) {
        m_mapping = tempMapping;
      } else {
        m_mapping = null;
      }
    } else {
      Mapping tempMapping = new AELHBaseMappingImpl();
      if ( tempMapping.loadXML( stepnode ) ) {
        m_mapping = tempMapping;
      } else {
        getLog().logError( "There is no meta data to inflate meta object" );
      }

      Node fields = XMLHandler.getSubNode( stepnode, "output_fields" );

      if ( fields != null ) {
        int nrfields = XMLHandler.countNodes( fields, "field" );
        List<HBaseValueMetaInterface> m_outputFields = new ArrayList<>( nrfields );

        for ( int i = 0; i < nrfields; i++ ) {
          m_outputFields.add( createFromNode( XMLHandler.getSubNodeByNr( fields, "field", i ) ) );
        }
      }
    }
  }

  private HBaseValueMetaInterface createFromNode( Node fieldNode ) {
    String isKey = XMLHandler.getTagValue( fieldNode, "key" ).trim();
    String alias = XMLHandler.getTagValue( fieldNode, "alias" ).trim();
    String columnFamily = "";
    String columnName = alias;
    if ( !isKey.equalsIgnoreCase( "Y" ) ) {
      if ( XMLHandler.getTagValue( fieldNode, "family" ) != null ) {
        columnFamily = XMLHandler.getTagValue( fieldNode, "family" ).trim();
      }

      if ( XMLHandler.getTagValue( fieldNode, "column" ) != null ) {
        columnName = XMLHandler.getTagValue( fieldNode, "column" ).trim();
      }
    }

    String typeS = XMLHandler.getTagValue( fieldNode, "type" ).trim();
    String tableName = XMLHandler.getTagValue( fieldNode, "table_name" );
    String mappingName = XMLHandler.getTagValue( fieldNode, "mapping_name" );

    AELHBaseValueMetaImpl vm = new AELHBaseValueMetaImpl( isKey.equalsIgnoreCase( "Y" ), alias, columnName, columnFamily, tableName, mappingName );
    vm.setHBaseTypeFromString( typeS );
    return vm;
  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws KettleException {

    if ( metaStore == null ) {
      metaStore = metaStoreService.getMetastore();
    }

    namedClusterLoadSaveUtil.saveRep( rep, metaStore, id_transformation, id_step, namedClusterService, namedCluster, getLog() );

    if ( !Const.isEmpty( m_coreConfigURL ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "core_config_url", m_coreConfigURL );
    }
    if ( !Const.isEmpty( m_defaultConfigURL ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "default_config_url", m_defaultConfigURL );
    }
    if ( !Const.isEmpty( m_sourceTableName ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "source_table_name", m_sourceTableName );
    }
    if ( !Const.isEmpty( m_sourceMappingName ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "source_mapping_name", m_sourceMappingName );
    }
    if ( !Const.isEmpty( m_keyStart ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "key_start", m_keyStart );
    }
    if ( !Const.isEmpty( m_keyStop ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "key_stop", m_keyStop );
    }
    if ( !Const.isEmpty( m_scannerCacheSize ) ) {
      rep.saveStepAttribute( id_transformation, id_step, 0, "scanner_cache_size", m_scannerCacheSize );
    }

    if ( m_outputFields != null && m_outputFields.size() > 0 ) {

      for ( int i = 0; i < m_outputFields.size(); i++ ) {
        m_outputFields.get( i ).saveRep( rep, id_transformation, id_step, i );
      }
    }

    if ( m_filters != null && m_filters.size() > 0 ) {
      for ( int i = 0; i < m_filters.size(); i++ ) {
        ColumnFilter f = m_filters.get( i );
        f.saveRep( rep, id_transformation, id_step, i );
      }
    }

    rep.saveStepAttribute( id_transformation, id_step, 0, "match_any_filter", m_matchAnyFilter );

    if ( m_mapping != null ) {
      m_mapping.saveRep( rep, id_transformation, id_step );
    }
  }

  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
      throws KettleException {

    if ( metaStore == null ) {
      metaStore = metaStoreService.getMetastore();
    }

    this.namedCluster = namedClusterLoadSaveUtil.loadClusterConfig( namedClusterService, id_step, rep, metaStore, null, getLog() );

    HBaseService hBaseService = null;
    try {
      hBaseService = getService();
    } catch ( Exception e ) {
      getLog().logError( e.getMessage() );
    }

    m_coreConfigURL = rep.getStepAttributeString( id_step, 0, "core_config_url" );
    m_defaultConfigURL = rep.getStepAttributeString( id_step, 0, "default_config_url" );
    m_sourceTableName = rep.getStepAttributeString( id_step, 0, "source_table_name" );
    m_sourceMappingName = rep.getStepAttributeString( id_step, 0, "source_mapping_name" );
    m_keyStart = rep.getStepAttributeString( id_step, 0, "key_start" );
    m_keyStop = rep.getStepAttributeString( id_step, 0, "key_stop" );
    m_matchAnyFilter = rep.getStepAttributeBoolean( id_step, 0, "match_any_filter" );
    m_scannerCacheSize = rep.getStepAttributeString( id_step, 0, "scanner_cache_size" );

    if ( hBaseService != null ) {
      HBaseValueMetaInterfaceFactory valueMetaInterfaceFactory = hBaseService.getHBaseValueMetaInterfaceFactory();
      ColumnFilterFactory columnFilterFactory = hBaseService.getColumnFilterFactory();
      MappingFactory mappingFactory = hBaseService.getMappingFactory();

      m_outputFields = valueMetaInterfaceFactory.createListFromRepository( rep, id_step );

      int nrFilters = rep.countNrStepAttributes( id_step, "cf_comparison_opp" );
      if ( nrFilters > 0 ) {
        m_filters = new ArrayList<>();

        for ( int i = 0; i < nrFilters; i++ ) {
          m_filters.add( columnFilterFactory.createFilter( rep, i, id_step ) );
        }
      }

      Mapping tempMapping = mappingFactory.createMapping();
      if ( tempMapping.readRep( rep, id_step ) ) {
        m_mapping = tempMapping;
      } else {
        m_mapping = null;
      }
    }
  }

  @Override
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
                     String[] input, String[] output, RowMetaInterface info, VariableSpace variableSpace, Repository repository, IMetaStore metaStore ) {

    if ( metaStore == null ) {
      metaStore = metaStoreService.getMetastore();
    }

    RowMeta r = new RowMeta();
    try {
      getFields( r, "testName", null, null, null, repository, metaStore );

      CheckResult cr =
          new CheckResult( CheckResult.TYPE_RESULT_OK, "Step can connect to HBase. Named mapping exists", stepMeta );
      remarks.add( cr );
    } catch ( Exception ex ) {
      CheckResult cr = new CheckResult( CheckResult.TYPE_RESULT_ERROR, ex.getMessage(), stepMeta );
      remarks.add( cr );
    }
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr,
                                TransMeta transMeta, Trans trans ) {

    return new HBaseInput( stepMeta, stepDataInterface, copyNr, transMeta, trans, namedClusterServiceLocator );
  }

  public StepDataInterface getStepData() {

    return new HBaseInputData();
  }

  private void setupCachedMapping( VariableSpace space ) throws KettleStepException {
    HBaseService hBaseService = null;
    try {
      hBaseService = getService();
    } catch ( ClusterInitializationException e ) {
      throw new KettleStepException( e );
    }
    if ( Const.isEmpty( m_coreConfigURL ) && Const.isEmpty( namedCluster.getZooKeeperHost() ) ) {
      throw new KettleStepException( "No output fields available (missing " + "connection details)!" );
    }

    if ( m_mapping == null && ( Const.isEmpty( m_sourceTableName ) || Const.isEmpty( m_sourceMappingName ) ) ) {
      throw new KettleStepException( "No output fields available (missing table " + "mapping details)!" );
    }

    if ( m_cachedMapping == null ) {
      // cache the mapping information
      if ( m_mapping != null ) {
        m_cachedMapping = m_mapping;
      } else {
        String coreConf = null;
        String defaultConf = null;

        try {
          if ( !Const.isEmpty( m_coreConfigURL ) ) {
            coreConf = space.environmentSubstitute( m_coreConfigURL );
          }
          if ( !Const.isEmpty( ( m_defaultConfigURL ) ) ) {
            defaultConf = space.environmentSubstitute( m_defaultConfigURL );
          }

        } catch ( Exception ex ) {
          throw new KettleStepException( ex.getMessage(), ex );
        }

        List<String> forLogging = new ArrayList<String>();

        try ( HBaseConnection conf = hBaseService.getHBaseConnection( space, coreConf, defaultConf, getLog() ) ) {
          MappingAdmin mappingAdmin = null;

          for ( String m : forLogging ) {
            logBasic( m );
          }

          mappingAdmin = new MappingAdmin( conf );

          m_cachedMapping = mappingAdmin.getMapping( m_sourceTableName, m_sourceMappingName );
        } catch ( Exception ex ) {
          throw new KettleStepException( ex.getMessage(), ex );
        }
      }
    }
  }

  @Override
  public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {

    rowMeta.clear(); // start afresh - eats the input

    if ( m_outputFields != null && m_outputFields.size() > 0 ) {
      // we have some stored field information - use this
      for ( HBaseValueMetaInterface vm : m_outputFields ) {

        vm.setOrigin( origin );
        rowMeta.addValueMeta( vm );
      }
    } else {
      // want all fields from the mapping - connect and get the details
      setupCachedMapping( space );

      int kettleType;
      if ( m_cachedMapping.getKeyType() == Mapping.KeyType.DATE
          || m_cachedMapping.getKeyType() == Mapping.KeyType.UNSIGNED_DATE ) {
        kettleType = ValueMetaInterface.TYPE_DATE;
      } else if ( m_cachedMapping.getKeyType() == Mapping.KeyType.STRING ) {
        kettleType = ValueMetaInterface.TYPE_STRING;
      } else if ( m_cachedMapping.getKeyType() == Mapping.KeyType.BINARY ) {
        kettleType = ValueMetaInterface.TYPE_BINARY;
      } else {
        kettleType = ValueMetaInterface.TYPE_INTEGER;
      }

      ValueMetaInterface keyMeta = new ValueMeta( m_cachedMapping.getKeyName(), kettleType );

      keyMeta.setOrigin( origin );
      rowMeta.addValueMeta( keyMeta );
      // }

      // Add the rest of the fields in the mapping
      Map<String, HBaseValueMetaInterface> mappedColumnsByAlias = m_cachedMapping.getMappedColumns();
      Set<String> aliasSet = mappedColumnsByAlias.keySet();
      for ( String alias : aliasSet ) {
        HBaseValueMetaInterface columnMeta = mappedColumnsByAlias.get( alias );
        columnMeta.setOrigin( origin );
        rowMeta.addValueMeta( columnMeta );
      }
    }
  }

  public NamedCluster getNamedCluster() {
    return namedCluster;
  }

  public void setNamedCluster( NamedCluster namedCluster ) {
    this.namedCluster = namedCluster;
  }

  public NamedClusterService getNamedClusterService() {
    return namedClusterService;
  }

  public NamedClusterServiceLocator getNamedClusterServiceLocator() {
    return namedClusterServiceLocator;
  }

  public RuntimeTestActionService getRuntimeTestActionService() {
    return runtimeTestActionService;
  }

  public RuntimeTester getRuntimeTester() {
    return runtimeTester;
  }

  public List<OutputFieldDefinition> getOutputFieldsDefinition() {
    return outputFieldsDefinition;
  }

  public void setOutputFieldsDefinition( List<OutputFieldDefinition> outputFieldsDefinition ) {
    this.outputFieldsDefinition = outputFieldsDefinition;
  }

  public List<FilterDefinition> getFiltersDefinition() {
    return filtersDefinition;
  }

  public void setFiltersDefinition( List<FilterDefinition> filtersDefinition ) {
    this.filtersDefinition = filtersDefinition;
  }

  public MappingDefinition getMappingDefinition() {
    return mappingDefinition;
  }

  public void setMappingDefinition( MappingDefinition mappingDefinition ) {
    this.mappingDefinition = mappingDefinition;
  }

  protected HBaseService getService() throws ClusterInitializationException {
    HBaseService service = null;
    try {
      service = namedClusterServiceLocator.getService( this.namedCluster, HBaseService.class );
      this.serviceStatus = ServiceStatus.OK;
    } catch ( Exception e ) {
      this.serviceStatus = ServiceStatus.notOk( e );
      logError( Messages.getString( "HBaseInput.Error.ServiceStatus" ) );
      throw e;
    }
    return service;
  }

  public ServiceStatus getServiceStatus() {
    if ( this.serviceStatus == null ) {
      this.serviceStatus = ServiceStatus.OK;
    }
    return this.serviceStatus;
  }
}
