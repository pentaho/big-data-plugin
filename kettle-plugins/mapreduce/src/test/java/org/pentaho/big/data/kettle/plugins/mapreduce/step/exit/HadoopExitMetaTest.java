/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2024 by Hitachi Vantara : http://www.pentaho.com
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

import org.junit.Before;
import org.junit.Test;
import org.pentaho.big.data.kettle.plugins.mapreduce.ui.step.exit.HadoopExitDialog;
import org.pentaho.di.core.bowl.DefaultBowl;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by bryan on 1/15/16.
 */
public class HadoopExitMetaTest {
  private HadoopExitMeta hadoopExitMeta;
  private IMetaStore metaStore;

  @Before
  public void setup() throws Throwable {
    metaStore = mock( IMetaStore.class );
    hadoopExitMeta = new HadoopExitMeta();
  }

  @Test
  public void testLoadSaveXml() throws Throwable {
    String outKeyField = "outKeyField";
    String outValField = "outValField";

    hadoopExitMeta.setOutKeyFieldname( outKeyField );
    hadoopExitMeta.setOutValueFieldname( outValField );

    Node node = DocumentBuilderFactory.newInstance().newDocumentBuilder()
      .parse( new InputSource( new StringReader( "<hem>" + hadoopExitMeta.getXML() + "</hem>" ) ) ).getFirstChild();
    hadoopExitMeta = new HadoopExitMeta();
    hadoopExitMeta.loadXML( node, new ArrayList<DatabaseMeta>(), metaStore );

    assertEquals( outKeyField, hadoopExitMeta.getOutKeyFieldname() );
    assertEquals( outValField, hadoopExitMeta.getOutValueFieldname() );
  }

  @Test
  public void testSaveRep() throws KettleException {
    ObjectId id_transformation = mock( ObjectId.class );
    ObjectId id_step = mock( ObjectId.class );
    Repository repository = mock( Repository.class );

    String outKeyField = "outKeyField";
    String outValField = "outValField";

    hadoopExitMeta.setOutKeyFieldname( outKeyField );
    hadoopExitMeta.setOutValueFieldname( outValField );

    hadoopExitMeta.saveRep( repository, metaStore, id_transformation, id_step );

    verify( repository ).saveStepAttribute( id_transformation, id_step, HadoopExitMeta.OUT_KEY_FIELDNAME, outKeyField );
    verify( repository )
      .saveStepAttribute( id_transformation, id_step, HadoopExitMeta.OUT_VALUE_FIELDNAME, outValField );
  }

  @Test
  public void testReadRep() throws KettleException {
    ObjectId id_step = mock( ObjectId.class );
    Repository repository = mock( Repository.class );

    String outKeyField = "outKeyField";
    String outValField = "outValField";

    when( repository.getStepAttributeString( id_step, HadoopExitMeta.OUT_KEY_FIELDNAME ) ).thenReturn( outKeyField );
    when( repository.getStepAttributeString( id_step, HadoopExitMeta.OUT_VALUE_FIELDNAME ) ).thenReturn( outValField );

    hadoopExitMeta.readRep( repository, metaStore, id_step, new ArrayList<DatabaseMeta>() );

    assertEquals( outKeyField, hadoopExitMeta.getOutKeyFieldname() );
    assertEquals( outValField, hadoopExitMeta.getOutValueFieldname() );

  }

  @Test
  public void testSetDefault() {
    String outKeyField = "outKeyField";
    String outValField = "outValField";

    hadoopExitMeta.setOutKeyFieldname( outKeyField );
    hadoopExitMeta.setOutValueFieldname( outValField );
    hadoopExitMeta.setDefault();

    assertNull( hadoopExitMeta.getOutKeyFieldname() );
    assertNull( hadoopExitMeta.getOutValueFieldname() );
  }

  @Test
  public void testGetFields() throws KettleStepException {
    String outKeyField = "outKeyField";
    String outValField = "outValField";

    hadoopExitMeta.setOutKeyFieldname( outKeyField );
    hadoopExitMeta.setOutValueFieldname( outValField );

    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );
    ValueMetaInterface key = mock( ValueMetaInterface.class );
    ValueMetaInterface keyClone = mock( ValueMetaInterface.class );
    ValueMetaInterface value = mock( ValueMetaInterface.class );
    ValueMetaInterface valueClone = mock( ValueMetaInterface.class );

    when( rowMetaInterface.searchValueMeta( outKeyField ) ).thenReturn( key );
    when( rowMetaInterface.searchValueMeta( outValField ) ).thenReturn( value );
    when( key.clone() ).thenReturn( keyClone );
    when( value.clone() ).thenReturn( valueClone );

    hadoopExitMeta.getFields( DefaultBowl.getInstance(), rowMetaInterface, null, null, null, null );

    verify( keyClone ).setName( HadoopExitMeta.OUT_KEY );
    verify( valueClone ).setName( HadoopExitMeta.OUT_VALUE );
    verify( rowMetaInterface ).clear();
    verify( rowMetaInterface ).addValueMeta( keyClone );
    verify( rowMetaInterface ).addValueMeta( valueClone );
  }

  @Test( expected = KettleStepException.class )
  public void testGetFieldsNullKey() throws KettleStepException {
    String outKeyField = "outKeyField";
    String outValField = "outValField";

    hadoopExitMeta.setOutKeyFieldname( outKeyField );
    hadoopExitMeta.setOutValueFieldname( outValField );

    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );

    try {
      hadoopExitMeta.getFields( DefaultBowl.getInstance(), rowMetaInterface, null, null, null, null );
    } catch ( KettleStepException e ) {
      assertEquals( BaseMessages.getString( HadoopExitMeta.PKG, HadoopExitMeta.ERROR_INVALID_KEY_FIELD, outKeyField ),
        e.getMessage().trim() );
      throw e;
    }
  }

  @Test( expected = KettleStepException.class )
  public void testGetFieldsNullValue() throws KettleStepException {
    String outKeyField = "outKeyField";
    String outValField = "outValField";

    hadoopExitMeta.setOutKeyFieldname( outKeyField );
    hadoopExitMeta.setOutValueFieldname( outValField );

    RowMetaInterface rowMetaInterface = mock( RowMetaInterface.class );
    ValueMetaInterface key = mock( ValueMetaInterface.class );
    when( rowMetaInterface.searchValueMeta( outKeyField ) ).thenReturn( key );

    try {
      hadoopExitMeta.getFields( DefaultBowl.getInstance(), rowMetaInterface, null, null, null, null );
    } catch ( KettleStepException e ) {
      assertEquals( BaseMessages.getString( HadoopExitMeta.PKG, HadoopExitMeta.ERROR_INVALID_VALUE_FIELD, outValField ),
        e.getMessage().trim() );
      throw e;
    }
  }

  private void assertSingleRemark( List<CheckResultInterface> remarks, int type, String text, StepMeta stepinfo ) {
    assertEquals( 1, remarks.size() );
    CheckResultInterface checkResultInterface = remarks.get( 0 );
    assertEquals( type, checkResultInterface.getType() );
    assertEquals( text, checkResultInterface.getText() );
    assertEquals( stepinfo, checkResultInterface.getSourceInfo() );
  }

  @Test
  public void testCheckNoDataStream() {
    List<CheckResultInterface> remarks = new ArrayList<>();
    StepMeta stepinfo = mock( StepMeta.class );
    hadoopExitMeta.check( remarks, null, stepinfo, null, null, null, null );
    assertSingleRemark( remarks, CheckResultInterface.TYPE_RESULT_ERROR,
      BaseMessages.getString( HadoopExitMeta.PKG, HadoopExitMeta.HADOOP_EXIT_META_CHECK_RESULT_NO_DATA_STREAM ),
      stepinfo );
  }

  @Test
  public void testCheckEmptyDataStream() {
    List<CheckResultInterface> remarks = new ArrayList<>();
    StepMeta stepinfo = mock( StepMeta.class );
    RowMetaInterface prev = mock( RowMetaInterface.class );
    when( prev.size() ).thenReturn( 0 );
    hadoopExitMeta.check( remarks, null, stepinfo, prev, null, null, null );
    assertSingleRemark( remarks, CheckResultInterface.TYPE_RESULT_ERROR,
      BaseMessages.getString( HadoopExitMeta.PKG, HadoopExitMeta.HADOOP_EXIT_META_CHECK_RESULT_NO_DATA_STREAM ),
      stepinfo );
  }

  @Test
  public void testCheckNoSpecifiedFieldsNullOutKeyFieldName() {
    List<CheckResultInterface> remarks = new ArrayList<>();
    StepMeta stepinfo = mock( StepMeta.class );
    when( stepinfo.getStepMetaInterface() ).thenReturn( hadoopExitMeta );
    RowMetaInterface prev = mock( RowMetaInterface.class );
    when( prev.size() ).thenReturn( 1 );
    when( prev.getFieldNames() ).thenReturn( new String[ 0 ] );
    hadoopExitMeta.check( remarks, null, stepinfo, prev, null, null, null );
    assertSingleRemark( remarks, CheckResultInterface.TYPE_RESULT_ERROR,
      BaseMessages.getString( HadoopExitMeta.PKG, HadoopExitMeta.HADOOP_EXIT_META_CHECK_RESULT_NO_SPECIFIED_FIELDS,
        prev.size() + "" ), stepinfo );
  }

  @Test
  public void testCheckNoSpecifiedFieldsNullOutValueFieldName() {
    List<CheckResultInterface> remarks = new ArrayList<>();
    StepMeta stepinfo = mock( StepMeta.class );
    String outKeyField = "outKeyField";
    hadoopExitMeta.setOutKeyFieldname( outKeyField );
    when( stepinfo.getStepMetaInterface() ).thenReturn( hadoopExitMeta );
    RowMetaInterface prev = mock( RowMetaInterface.class );
    when( prev.size() ).thenReturn( 1 );
    when( prev.getFieldNames() ).thenReturn( new String[ 0 ] );
    hadoopExitMeta.check( remarks, null, stepinfo, prev, null, null, null );
    assertSingleRemark( remarks, CheckResultInterface.TYPE_RESULT_ERROR,
      BaseMessages.getString( HadoopExitMeta.PKG, HadoopExitMeta.HADOOP_EXIT_META_CHECK_RESULT_NO_SPECIFIED_FIELDS,
        prev.size() + "" ), stepinfo );
  }

  @Test
  public void testCheckNotReceivingSpecifiedKeyFields() {
    List<CheckResultInterface> remarks = new ArrayList<>();
    StepMeta stepinfo = mock( StepMeta.class );
    String outKeyField = "outKeyField";
    String outValField = "outValField";
    hadoopExitMeta.setOutKeyFieldname( outKeyField );
    hadoopExitMeta.setOutValueFieldname( outValField );
    when( stepinfo.getStepMetaInterface() ).thenReturn( hadoopExitMeta );
    RowMetaInterface prev = mock( RowMetaInterface.class );
    when( prev.size() ).thenReturn( 1 );
    when( prev.getFieldNames() ).thenReturn( new String[ 0 ] );
    hadoopExitMeta.check( remarks, null, stepinfo, prev, null, null, null );
    assertSingleRemark( remarks, CheckResultInterface.TYPE_RESULT_ERROR,
      BaseMessages
        .getString( HadoopExitMeta.PKG, HadoopExitMeta.HADOOP_EXIT_META_CHECK_RESULT_NOT_RECEVING_SPECIFIED_FIELDS,
          prev.size() + "" ), stepinfo );
  }

  @Test
  public void testCheckNotReceivingSpecifiedValFields() {
    List<CheckResultInterface> remarks = new ArrayList<>();
    StepMeta stepinfo = mock( StepMeta.class );
    String outKeyField = "outKeyField";
    String outValField = "outValField";
    hadoopExitMeta.setOutKeyFieldname( outKeyField );
    hadoopExitMeta.setOutValueFieldname( outValField );
    when( stepinfo.getStepMetaInterface() ).thenReturn( hadoopExitMeta );
    RowMetaInterface prev = mock( RowMetaInterface.class );
    when( prev.size() ).thenReturn( 1 );
    when( prev.getFieldNames() ).thenReturn( new String[] { outKeyField } );
    hadoopExitMeta.check( remarks, null, stepinfo, prev, null, null, null );
    assertSingleRemark( remarks, CheckResultInterface.TYPE_RESULT_ERROR,
      BaseMessages
        .getString( HadoopExitMeta.PKG, HadoopExitMeta.HADOOP_EXIT_META_CHECK_RESULT_NOT_RECEVING_SPECIFIED_FIELDS,
          prev.size() + "" ), stepinfo );
  }

  @Test
  public void testCheckOk() {
    List<CheckResultInterface> remarks = new ArrayList<>();
    StepMeta stepinfo = mock( StepMeta.class );
    String outKeyField = "outKeyField";
    String outValField = "outValField";
    hadoopExitMeta.setOutKeyFieldname( outKeyField );
    hadoopExitMeta.setOutValueFieldname( outValField );
    when( stepinfo.getStepMetaInterface() ).thenReturn( hadoopExitMeta );
    RowMetaInterface prev = mock( RowMetaInterface.class );
    when( prev.size() ).thenReturn( 1 );
    when( prev.getFieldNames() ).thenReturn( new String[] { outKeyField, outValField } );
    hadoopExitMeta.check( remarks, null, stepinfo, prev, null, null, null );
    assertSingleRemark( remarks, CheckResultInterface.TYPE_RESULT_OK,
      BaseMessages
        .getString( HadoopExitMeta.PKG, HadoopExitMeta.HADOOP_EXIT_META_CHECK_RESULT_STEP_RECEVING_DATA,
          prev.size() + "" ), stepinfo );
  }

  @Test
  public void testGetStep() {
    StepMeta stepMeta = mock( StepMeta.class );
    String testName = "testName";
    when( stepMeta.getName() ).thenReturn( testName );
    TransMeta transMeta = mock( TransMeta.class );
    when( transMeta.findStep( testName ) ).thenReturn( stepMeta );
    assertTrue(
      hadoopExitMeta.getStep( stepMeta, null, 0, transMeta, mock( Trans.class ) ) instanceof HadoopExit );
  }

  @Test
  public void testClone() {
    assertTrue( hadoopExitMeta.clone() instanceof HadoopExitMeta );
  }

  @Test
  public void testGetStepData() {
    assertTrue( hadoopExitMeta.getStepData() instanceof HadoopExitData );
  }

  @Test
  public void testGetDialogClassName() {
    assertEquals( HadoopExitDialog.class.getCanonicalName(), hadoopExitMeta.getDialogClassName() );
  }
}
