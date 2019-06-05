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

package org.pentaho.big.data.kettle.plugins.mapreduce.step.exit;

import org.pentaho.big.data.kettle.plugins.mapreduce.DialogClassUtil;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.List;

@Step( id = "HadoopExitPlugin", image = "MRO.svg", name = "HadoopExitPlugin.Name",
    description = "HadoopExitPlugin.Description",
    documentationUrl = "Products/MapReduce_Output",
    categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.BigData",
    i18nPackageName = "org.pentaho.di.trans.steps.hadoopexit" )
@InjectionSupported( localizationPrefix = "HadoopExitPlugin.Injection." )
public class HadoopExitMeta extends BaseStepMeta implements StepMetaInterface {
  public static final String ERROR_INVALID_KEY_FIELD = "Error.InvalidKeyField";
  public static final String ERROR_INVALID_VALUE_FIELD = "Error.InvalidValueField";
  public static final String OUT_KEY = "outKey";
  public static final String OUT_VALUE = "outValue";
  public static final String HADOOP_EXIT_META_CHECK_RESULT_NO_DATA_STREAM = "HadoopExitMeta.CheckResult.NoDataStream";
  public static final String HADOOP_EXIT_META_CHECK_RESULT_NO_SPECIFIED_FIELDS =
    "HadoopExitMeta.CheckResult.NoSpecifiedFields";
  public static final String HADOOP_EXIT_META_CHECK_RESULT_STEP_RECEVING_DATA =
    "HadoopExitMeta.CheckResult.StepRecevingData";
  public static final String HADOOP_EXIT_META_CHECK_RESULT_NOT_RECEVING_SPECIFIED_FIELDS =
    "HadoopExitMeta.CheckResult.NotRecevingSpecifiedFields";
  public static Class<?> PKG = HadoopExit.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$
  public static final String DIALOG_NAME = DialogClassUtil.getDialogClassName( PKG );

  public static String OUT_KEY_FIELDNAME = "outkeyfieldname";

  public static String OUT_VALUE_FIELDNAME = "outvaluefieldname";

  @Injection( name = "KEY_FIELD" )
  private String outKeyFieldname;

  @Injection( name = "VALUE_FIELD" )
  private String outValueFieldname;

  public HadoopExitMeta() throws Throwable {
    super();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore )
    throws KettleXMLException {
    setOutKeyFieldname( XMLHandler.getTagValue( stepnode, HadoopExitMeta.OUT_KEY_FIELDNAME ) ); //$NON-NLS-1$
    setOutValueFieldname( XMLHandler.getTagValue( stepnode, HadoopExitMeta.OUT_VALUE_FIELDNAME ) ); //$NON-NLS-1$
  }

  @Override public String getXML() {
    StringBuilder retval = new StringBuilder();

    retval.append( "    " ).append( XMLHandler.addTagValue( HadoopExitMeta.OUT_KEY_FIELDNAME, getOutKeyFieldname() ) );
    retval.append( "    " )
        .append( XMLHandler.addTagValue( HadoopExitMeta.OUT_VALUE_FIELDNAME, getOutValueFieldname() ) );

    return retval.toString();
  }

  public Object clone() {
    return super.clone();
  }

  @Override public void setDefault() {
    setOutKeyFieldname( null );
    setOutValueFieldname( null );
  }

  @Override public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {
    setOutKeyFieldname( rep.getStepAttributeString( id_step, HadoopExitMeta.OUT_KEY_FIELDNAME ) ); //$NON-NLS-1$
    setOutValueFieldname( rep.getStepAttributeString( id_step, HadoopExitMeta.OUT_VALUE_FIELDNAME ) ); //$NON-NLS-1$
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step ) throws KettleException {
    rep.saveStepAttribute( id_transformation, id_step, HadoopExitMeta.OUT_KEY_FIELDNAME, getOutKeyFieldname() ); //$NON-NLS-1$
    rep.saveStepAttribute( id_transformation, id_step, HadoopExitMeta.OUT_VALUE_FIELDNAME, getOutValueFieldname() ); //$NON-NLS-1$
  }

  @Override public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
      VariableSpace space ) throws KettleStepException {

    ValueMetaInterface key = rowMeta.searchValueMeta( getOutKeyFieldname() );
    ValueMetaInterface value = rowMeta.searchValueMeta( getOutValueFieldname() );

    if ( key == null ) {
      throw new KettleStepException( BaseMessages.getString( PKG, ERROR_INVALID_KEY_FIELD, getOutKeyFieldname() ) );
    }
    if ( value == null ) {
      throw new KettleStepException( BaseMessages.getString( PKG, ERROR_INVALID_VALUE_FIELD, getOutValueFieldname() ) );
    }

    // The output consists of 2 fields: outKey and outValue
    // The data types rely on the input data type so we look those up
    //
    ValueMetaInterface keyMeta = key.clone();
    ValueMetaInterface valueMeta = value.clone();

    keyMeta.setName( OUT_KEY );
    valueMeta.setName( OUT_VALUE );

    rowMeta.clear();

    rowMeta.addValueMeta( keyMeta );
    rowMeta.addValueMeta( valueMeta );
  }

  @Override
  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepinfo, RowMetaInterface prev,
      String[] input, String[] output, RowMetaInterface info ) {
    CheckResult cr;

    // Make sure we have an input stream that contains the desired field names
    if ( prev == null || prev.size() == 0 ) {
      cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
            HADOOP_EXIT_META_CHECK_RESULT_NO_DATA_STREAM ), stepinfo ); //$NON-NLS-1$
      remarks.add( cr );
    } else {
      List<String> fieldnames = Arrays.asList( prev.getFieldNames() );

      HadoopExitMeta stepMeta = (HadoopExitMeta) stepinfo.getStepMetaInterface();

      if ( ( stepMeta.getOutKeyFieldname() == null ) || stepMeta.getOutValueFieldname() == null ) {
        cr =
            new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
              HADOOP_EXIT_META_CHECK_RESULT_NO_SPECIFIED_FIELDS, prev.size() + "" ), stepinfo ); //$NON-NLS-1$ //$NON-NLS-2$
        remarks.add( cr );
      } else {

        if ( fieldnames.contains( stepMeta.getOutKeyFieldname() )
            && fieldnames.contains( stepMeta.getOutValueFieldname() ) ) {
          cr =
              new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
                HADOOP_EXIT_META_CHECK_RESULT_STEP_RECEVING_DATA, prev.size() + "" ), stepinfo ); //$NON-NLS-1$ //$NON-NLS-2$
          remarks.add( cr );
        } else {
          cr =
              new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
                HADOOP_EXIT_META_CHECK_RESULT_NOT_RECEVING_SPECIFIED_FIELDS, prev.size() + "" ), stepinfo ); //$NON-NLS-1$ //$NON-NLS-2$
          remarks.add( cr );
        }
      }
    }
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
      Trans trans ) {
    return new HadoopExit( stepMeta, stepDataInterface, cnr, tr, trans );
  }

  public StepDataInterface getStepData() {
    return new HadoopExitData();
  }

  public String getOutKeyFieldname() {
    return outKeyFieldname;
  }

  public void setOutKeyFieldname( String arg ) {
    outKeyFieldname = arg;
  }

  public String getOutValueFieldname() {
    return outValueFieldname;
  }

  public void setOutValueFieldname( String arg ) {
    outValueFieldname = arg;
  }

  @Override public String getDialogClassName() {
    return DIALOG_NAME;
  }
}
