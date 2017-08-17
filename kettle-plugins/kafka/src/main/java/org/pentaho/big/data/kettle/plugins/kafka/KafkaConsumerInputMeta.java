/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.pentaho.big.data.kettle.plugins.kafka;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.cluster.service.locator.NamedClusterServiceLocator;
import org.pentaho.bigdata.api.jaas.JaasConfigService;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.ObjectLocationSpecificationMethod;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionDeep;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.StepWithMappingMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.osgi.metastore.locator.api.MetastoreLocator;
import org.w3c.dom.Node;

/**
 * Skeleton for PDI Step plugin.
 */
@Step( id = "KafkaConsumerInput", image = "KafkaConsumerInput.svg", name = "Kafka Consumer",
  description = "Consume messages from a Kafka topic", categoryDescription = "Input" )
@InjectionSupported( localizationPrefix = "KafkaConsumerInputMeta.Injection." )
public class KafkaConsumerInputMeta extends StepWithMappingMeta implements StepMetaInterface {

  public static final String CLUSTER_NAME = "clusterName";
  public static final String TOPIC = "topic";
  public static final String CONSUMER_GROUP = "consumerGroup";
  public static final String TRANSFORMATION_PATH = "transformationPath";
  public static final String BATCH_SIZE = "batchSize";
  public static final String BATCH_DURATION = "batchDuration";
  public static final String ADVANCED_CONFIG = "advancedConfig";

  public static final String TOPIC_FIELD_NAME = TOPIC;
  public static final String OFFSET_FIELD_NAME = "offset";
  public static final String PARTITION_FIELD_NAME = "partition";

  public static final String TIMESTAMP_FIELD_NAME = "timestamp";
  public static final String OUTPUT_FIELD_TAG_NAME = "OutputField";
  public static final String KAFKA_NAME_ATTRIBUTE = "kafkaName";
  public static final String TYPE_ATTRIBUTE = "type";

  private static Class<?> PKG = KafkaConsumerInput.class; // for i18n purposes, needed by Translator2!!   $NON-NLS-1$

  @Injection( name = "TRANSFORMATION_PATH" )
  private String transformationPath;

  @Injection( name = "CLUSTER_NAME" )
  private String clusterName;

  @Injection( name = "TOPICS" )
  private List<String> topics = new ArrayList<>();

  @Injection( name = "CONSUMER_GROUP" )
  private String consumerGroup;

  @Injection( name = "NUM_MESSAGES" )
  private long batchSize;

  @Injection( name = "DURATION" )
  private long batchDuration;

  private Map<String, String> advancedConfig = new LinkedHashMap<>();

  @InjectionDeep( prefix = "KEY" ) private KafkaConsumerField keyField;
  @InjectionDeep( prefix = "MESSAGE" ) private KafkaConsumerField messageField;
  private KafkaConsumerField topicField;
  private KafkaConsumerField offsetField;
  private KafkaConsumerField partitionField;
  private KafkaConsumerField timestampField;

  private transient KafkaFactory kafkaFactory;
  private NamedClusterService namedClusterService;
  private MetastoreLocator metastoreLocator;
  private NamedClusterServiceLocator namedClusterServiceLocator;

  public KafkaConsumerInputMeta() {
    super(); // allocate BaseStepMeta
    kafkaFactory = KafkaFactory.defaultFactory();
    keyField = new KafkaConsumerField(
      KafkaConsumerField.Name.KEY,
      BaseMessages.getString( PKG, "KafkaConsumerInputDialog.KeyField" )
    );

    messageField = new KafkaConsumerField(
      KafkaConsumerField.Name.MESSAGE,
      BaseMessages.getString( PKG, "KafkaConsumerInputDialog.MessageField" )
    );

    topicField = new KafkaConsumerField(
      KafkaConsumerField.Name.TOPIC,
      BaseMessages.getString( PKG, "KafkaConsumerInputDialog.TopicField" )
    );

    partitionField = new KafkaConsumerField(
      KafkaConsumerField.Name.PARTITION,
      BaseMessages.getString( PKG, "KafkaConsumerInputDialog.PartitionField" ),
      KafkaConsumerField.Type.Integer
    );

    offsetField = new KafkaConsumerField(
      KafkaConsumerField.Name.OFFSET,
      BaseMessages.getString( PKG, "KafkaConsumerInputDialog.OffsetField" ),
      KafkaConsumerField.Type.Integer
    );

    timestampField = new KafkaConsumerField(
      KafkaConsumerField.Name.TIMESTAMP,
      BaseMessages.getString( PKG, "KafkaConsumerInputDialog.TimestampField" ),
      KafkaConsumerField.Type.Integer
    );
    setSpecificationMethod( ObjectLocationSpecificationMethod.FILENAME );
  }

  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    readData( stepnode );
  }

  public Object clone() {
    Object retval = super.clone();
    return retval;
  }

  private void readData( Node stepnode ) {
    setClusterName( XMLHandler.getTagValue( stepnode, CLUSTER_NAME ) );

    List<Node> topicsNode = XMLHandler.getNodes( stepnode, TOPIC );
    topicsNode.forEach( node -> {
      String displayName = XMLHandler.getNodeValue( node );
      addTopic( displayName );
    } );

    setConsumerGroup( XMLHandler.getTagValue( stepnode, CONSUMER_GROUP ) );
    setTransformationPath( XMLHandler.getTagValue( stepnode, TRANSFORMATION_PATH ) );
    setBatchSize( Long.parseLong( XMLHandler.getTagValue( stepnode, BATCH_SIZE ) ) );
    setBatchDuration( Long.parseLong( XMLHandler.getTagValue( stepnode, BATCH_DURATION ) ) );
    List<Node> ofNode = XMLHandler.getNodes( stepnode, OUTPUT_FIELD_TAG_NAME );

    ofNode.forEach( node -> {
      String displayName = XMLHandler.getNodeValue( node );
      String kafkaName = XMLHandler.getTagAttribute( node, KAFKA_NAME_ATTRIBUTE );
      String type = XMLHandler.getTagAttribute( node, TYPE_ATTRIBUTE );
      KafkaConsumerField field = new KafkaConsumerField(
        KafkaConsumerField.Name.valueOf( kafkaName.toUpperCase() ),
        displayName,
        KafkaConsumerField.Type.valueOf( type ) );

      setField( field );
    } );

    advancedConfig = new LinkedHashMap<>();

    Optional.ofNullable( XMLHandler.getSubNode( stepnode, ADVANCED_CONFIG ) ).map( node -> node.getChildNodes() )
        .ifPresent( nodes -> IntStream.range( 0, nodes.getLength() ).mapToObj( nodes::item )
            .filter( node -> node.getNodeType() == Node.ELEMENT_NODE )
            .forEach( node -> advancedConfig.put( node.getNodeName(), node.getTextContent() ) ) );
  }

  protected void setField( KafkaConsumerField field ) {
    field.getKafkaName().setFieldOnMeta( this, field );
  }

  public void setDefault() {
    batchSize = 1000;
    batchDuration = 0;
  }

  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {
    setClusterName( rep.getStepAttributeString( id_step, CLUSTER_NAME ) );

    int topicCount = rep.countNrStepAttributes( id_step, TOPIC );
    for ( int i = 0; i < topicCount; i++ ) {
      addTopic( rep.getStepAttributeString( id_step, i, TOPIC ) );
    }

    setConsumerGroup( rep.getStepAttributeString( id_step, CONSUMER_GROUP ) );
    setTransformationPath( rep.getStepAttributeString( id_step, TRANSFORMATION_PATH ) );
    setBatchSize( rep.getStepAttributeInteger( id_step, BATCH_SIZE ) );
    setBatchDuration( rep.getStepAttributeInteger( id_step, BATCH_DURATION ) );

    for ( KafkaConsumerField.Name name : KafkaConsumerField.Name.values() ) {
      String prefix = OUTPUT_FIELD_TAG_NAME + "_" + name;
      String value = rep.getStepAttributeString( id_step, prefix );
      String type = rep.getStepAttributeString( id_step, prefix + "_" + TYPE_ATTRIBUTE );
      if ( value != null ) {
        setField( new KafkaConsumerField( name, value, KafkaConsumerField.Type.valueOf( type ) ) );
      }
    }

    advancedConfig = new LinkedHashMap<>();

    for ( int i = 0; i < rep.getStepAttributeInteger( id_step, ADVANCED_CONFIG + "_COUNT" ); i++ ) {
      advancedConfig.put( rep.getStepAttributeString( id_step, i, ADVANCED_CONFIG + "_NAME" ),
          rep.getStepAttributeString( id_step, i, ADVANCED_CONFIG + "_VALUE" ) );
    }
  }

  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId transId, ObjectId stepId )
    throws KettleException {
    rep.saveStepAttribute( transId, stepId, CLUSTER_NAME, clusterName );

    int i = 0;
    for ( String topic : topics ) {
      rep.saveStepAttribute( transId, stepId, i++, TOPIC, topic );
    }

    rep.saveStepAttribute( transId, stepId, CONSUMER_GROUP, consumerGroup );
    rep.saveStepAttribute( transId, stepId, TRANSFORMATION_PATH, transformationPath );
    rep.saveStepAttribute( transId, stepId, BATCH_SIZE, batchSize );
    rep.saveStepAttribute( transId, stepId, BATCH_DURATION, batchDuration );

    List<KafkaConsumerField> fields = getFieldDefinitions();
    for ( KafkaConsumerField field : fields ) {
      String prefix = OUTPUT_FIELD_TAG_NAME + "_" + field.getKafkaName().toString();
      rep.saveStepAttribute( transId, stepId, prefix, field.getOutputName() );
      rep.saveStepAttribute( transId, stepId, prefix + "_" + TYPE_ATTRIBUTE, field.getOutputType().toString() );
    }

    rep.saveStepAttribute( transId, stepId, ADVANCED_CONFIG + "_COUNT", getAdvancedConfig().size() );

    i = 0;
    for ( String propName : getAdvancedConfig().keySet() ) {
      rep.saveStepAttribute( transId, stepId, i, ADVANCED_CONFIG + "_NAME", propName );
      rep.saveStepAttribute( transId, stepId, i++, ADVANCED_CONFIG + "_VALUE", getAdvancedConfig().get( propName ) );
    }
  }

  public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {

    putFieldOnRowMeta( getKeyField(), rowMeta, origin, space );
    putFieldOnRowMeta( getMessageField(), rowMeta, origin, space );
    putFieldOnRowMeta( getTopicField(), rowMeta, origin, space );
    putFieldOnRowMeta( getPartitionField(), rowMeta, origin, space );
    putFieldOnRowMeta( getOffsetField(), rowMeta, origin, space );
    putFieldOnRowMeta( getTimestampField(), rowMeta, origin, space );
  }

  private void putFieldOnRowMeta( KafkaConsumerField field, RowMetaInterface rowMeta,
                                  String origin, VariableSpace space ) throws KettleStepException {
    if ( field != null && !Utils.isEmpty( field.getOutputName() ) ) {
      try {
        String value = space.environmentSubstitute( field.getOutputName() );
        ValueMetaInterface v = ValueMetaFactory.createValueMeta( value,
          field.getOutputType().getValueMetaInterfaceType() );
        v.setOrigin( origin );
        rowMeta.addValueMeta( v );
      } catch ( KettlePluginException e ) {
        throw new KettleStepException( BaseMessages.getString(
          PKG,
          "KafkaConsumerInputMeta.UnableToCreateValueType",
          field
        ), e );
      }
    }
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta,
                     StepMeta stepMeta, RowMetaInterface prev, String[] input, String[] output,
                     RowMetaInterface info, VariableSpace space, Repository repository,
                     IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING,
        BaseMessages.getString( PKG, "KafkaConsumerInputMeta.CheckResult.NotReceivingFields" ), stepMeta );
      remarks.add( cr );
    } else {
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_OK,
        BaseMessages.getString( PKG, "KafkaConsumerInputMeta.CheckResult.StepRecevingData", prev.size() + "" ),
        stepMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_OK,
        BaseMessages.getString( PKG, "KafkaConsumerInputMeta.CheckResult.StepRecevingData2" ), stepMeta );
      remarks.add( cr );
    } else {
      cr = new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR,
        BaseMessages.getString( PKG, "KafkaConsumerInputMeta.CheckResult.NoInputReceivedFromOtherSteps" ), stepMeta );
      remarks.add( cr );
    }
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
                                Trans trans ) {
    return new KafkaConsumerInput( stepMeta, stepDataInterface, cnr, tr, trans );
  }

  public StepDataInterface getStepData() {
    return new KafkaConsumerInputData();
  }

  public String getDialogClassName() {
    return "org.pentaho.big.data.kettle.plugins.kafka.KafkaConsumerInputDialog";
  }

  public void setTopics( ArrayList<String> topics ) {
    this.topics = topics;
  }

  public void addTopic( String topic ) {
    this.topics.add( topic );
  }

  public void setConsumerGroup( String consumerGroup ) {
    this.consumerGroup = consumerGroup;
  }

  public String getBootstrapServers() {
    return Optional
        .ofNullable( namedClusterService.getNamedClusterByName( clusterName, metastoreLocator.getMetastore() ) )
        .map( nc -> nc.getKafkaBootstrapServers() ).orElse( "" );
  }

  public List<String> getTopics() {
    return topics;
  }

  public String getConsumerGroup() {
    return consumerGroup;
  }

  public KafkaConsumerField getKeyField() {
    return keyField;
  }

  public KafkaConsumerField getMessageField() {
    return messageField;
  }

  public KafkaConsumerField getTopicField() {
    return topicField;
  }

  public KafkaConsumerField getOffsetField() {
    return offsetField;
  }

  public KafkaConsumerField getPartitionField() {
    return partitionField;
  }

  public KafkaConsumerField getTimestampField() {
    return timestampField;
  }

  public String getTransformationPath() {
    return transformationPath;
  }

  public long getBatchSize() {
    return batchSize;
  }

  public long getBatchDuration() {
    return batchDuration;
  }

  public void setKeyField( KafkaConsumerField keyField ) {
    this.keyField = keyField;
  }

  public void setMessageField( KafkaConsumerField messageField ) {
    this.messageField = messageField;
  }

  public void setTopicField( KafkaConsumerField topicField ) {
    this.topicField = topicField;
  }

  public void setOffsetField( KafkaConsumerField offsetField ) {
    this.offsetField = offsetField;
  }

  public void setPartitionField( KafkaConsumerField partitionField ) {
    this.partitionField = partitionField;
  }

  public void setTimestampField( KafkaConsumerField timestampField ) {
    this.timestampField = timestampField;
  }

  public void setTransformationPath( String transformationPath ) {
    this.transformationPath = transformationPath;
  }

  public void setBatchSize( long batchSize ) {
    this.batchSize = batchSize;
  }

  public void setBatchDuration( long batchDuration ) {
    this.batchDuration = batchDuration;
  }

  @Override public String getXML() throws KettleException {
    StringBuilder retval = new StringBuilder();
    retval.append( "    " ).append( XMLHandler.addTagValue( CLUSTER_NAME, clusterName ) );

    getTopics().forEach( topic ->
      retval.append( "    " ).append( XMLHandler.addTagValue( TOPIC, topic ) ) );

    retval.append( "    " ).append( XMLHandler.addTagValue( CONSUMER_GROUP, consumerGroup ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( TRANSFORMATION_PATH, transformationPath ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( BATCH_SIZE, batchSize ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( BATCH_DURATION, batchDuration ) );

    getFieldDefinitions().forEach( field ->
      retval.append( "    " ).append(
        XMLHandler.addTagValue( OUTPUT_FIELD_TAG_NAME, field.getOutputName(), true,
          KAFKA_NAME_ATTRIBUTE, field.getKafkaName().toString(),
          TYPE_ATTRIBUTE, field.getOutputType().toString() ) ) );

    retval.append( "    " ).append( XMLHandler.openTag( ADVANCED_CONFIG ) ).append( Const.CR );
    getAdvancedConfig().forEach( ( key, value ) -> retval.append( "        " )
        .append( XMLHandler.addTagValue( (String) key, (String) value ) ) );
    retval.append( "    " ).append( XMLHandler.closeTag( ADVANCED_CONFIG ) ).append( Const.CR );

    return retval.toString();
  }

  public List<KafkaConsumerField> getFieldDefinitions() {
    return Lists.newArrayList(
      getKeyField(),
      getMessageField(),
      getTopicField(),
      getPartitionField(),
      getOffsetField(),
      getTimestampField() );
  }

  public KafkaFactory getKafkaFactory() {
    return kafkaFactory;
  }

  void setKafkaFactory( KafkaFactory kafkaFactory ) {
    this.kafkaFactory = kafkaFactory;
  }

  public NamedClusterService getNamedClusterService() {
    return namedClusterService;
  }

  public String getClusterName() {
    return clusterName;
  }

  public void setClusterName( String clusterName ) {
    this.clusterName = clusterName;
  }

  public MetastoreLocator getMetastoreLocator() {
    return metastoreLocator;
  }

  public void setNamedClusterService( NamedClusterService namedClusterService ) {
    this.namedClusterService = namedClusterService;
  }

  public Optional<JaasConfigService> getJaasConfigService() {
    try {
      return Optional.ofNullable( namedClusterServiceLocator.getService(
        namedClusterService.getNamedClusterByName( getClusterName(), getMetastoreLocator().getMetastore() ),
        JaasConfigService.class ) );
    } catch ( Exception e ) {
      log.logDebug( "problem getting jaas config", e );
      return Optional.empty();
    }
  }

  public NamedClusterServiceLocator getNamedClusterServiceLocator() {
    return namedClusterServiceLocator;
  }

  public void setNamedClusterServiceLocator( NamedClusterServiceLocator namedClusterServiceLocator ) {
    this.namedClusterServiceLocator = namedClusterServiceLocator;
  }

  public void setMetastoreLocator( MetastoreLocator metastoreLocator ) {
    this.metastoreLocator = metastoreLocator;
  }

  public void setAdvancedConfig( Map<String, String> config ) {
    advancedConfig = config;
  }

  public Map<String, String> getAdvancedConfig() {
    return advancedConfig;
  }
}
