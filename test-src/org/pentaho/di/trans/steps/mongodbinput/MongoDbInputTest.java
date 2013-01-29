package org.pentaho.di.trans.steps.mongodbinput;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaPluginType;
import org.pentaho.di.core.variables.Variables;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

public class MongoDbInputTest {
  protected static String s_testData = "{\"one\" : {\"three\" : [ {\"rec2\" : { \"f0\" : \"zzz\" } } ], "
      + "\"two\" : [ { \"rec1\" : { \"f1\" : \"bob\", \"f2\" : \"fred\" } } ] }, "
      + "\"name\" : \"george\", \"aNumber\" : 42 }";
  protected static String s_testData2 = "{\"one\" : {\"three\" : [ {\"rec2\" : { \"f0\" : \"zzz\" } } ], "
      + "\"two\" : [ { \"rec1\" : { \"f1\" : \"bob\", \"f2\" : \"fred\" } } ] }, "
      + "\"name\" : \"george\", \"aNumber\" : \"Forty two\" }";
  protected static String s_testData3 = "{\"one\" : {\"three\" : [ {\"rec2\" : { \"f0\" : \"zzz\" } } ], "
      + "\"two\" : [ { \"rec1\" : { \"f1\" : \"bob\", \"f2\" : \"fred\" } }, "
      + "{ \"rec1\" : { \"f1\" : \"sid\", \"f2\" : \"zaphod\" } } ] }, "
      + "\"name\" : \"george\", \"aNumber\" : \"Forty two\" }";

  static {
    try {
      ValueMetaPluginType.getInstance().searchPlugins();
    } catch (KettlePluginException ex) {
      ex.printStackTrace();
    }
  }

  @Test
  public void testDeterminePaths() {
    Map<String, MongoDbInputData.MongoField> fieldLookup = new HashMap<String, MongoDbInputData.MongoField>();
    List<MongoDbInputData.MongoField> discoveredFields = new ArrayList<MongoDbInputData.MongoField>();

    Object mongoO = JSON.parse(s_testData);
    assertTrue(mongoO instanceof DBObject);

    MongoDbInputData.docToFields((DBObject) mongoO, fieldLookup);
    MongoDbInputData.postProcessPaths(fieldLookup, discoveredFields, 1);

    assertEquals(5, discoveredFields.size());

    // check types
    int stringCount = 0;
    int numCount = 0;
    for (MongoDbInputData.MongoField m : discoveredFields) {
      if (ValueMeta.getType(m.m_kettleType) == ValueMetaInterface.TYPE_STRING) {
        stringCount++;
      }

      if (ValueMeta.getType(m.m_kettleType) == ValueMetaInterface.TYPE_INTEGER) {
        numCount++;
      }
    }

    assertEquals(numCount, 1);
    assertEquals(stringCount, 4);
  }

  @Test
  public void testDeterminePathsWithDisparateTypes() {
    Map<String, MongoDbInputData.MongoField> fieldLookup = new HashMap<String, MongoDbInputData.MongoField>();
    List<MongoDbInputData.MongoField> discoveredFields = new ArrayList<MongoDbInputData.MongoField>();

    Object mongoO = JSON.parse(s_testData);
    assertTrue(mongoO instanceof DBObject);
    MongoDbInputData.docToFields((DBObject) mongoO, fieldLookup);

    mongoO = JSON.parse(s_testData2);
    assertTrue(mongoO instanceof DBObject);
    MongoDbInputData.docToFields((DBObject) mongoO, fieldLookup);

    MongoDbInputData.postProcessPaths(fieldLookup, discoveredFields, 1);

    assertEquals(5, discoveredFields.size());
    Collections.sort(discoveredFields);

    // First path is the "aNumber" field
    assertTrue(discoveredFields.get(0).m_disparateTypes);
  }

  @Test
  public void testGetAllFields() throws KettleException {

    Map<String, MongoDbInputData.MongoField> fieldLookup = new HashMap<String, MongoDbInputData.MongoField>();
    List<MongoDbInputData.MongoField> discoveredFields = new ArrayList<MongoDbInputData.MongoField>();

    Object mongoO = JSON.parse(s_testData);
    assertTrue(mongoO instanceof DBObject);

    MongoDbInputData.docToFields((DBObject) mongoO, fieldLookup);
    MongoDbInputData.postProcessPaths(fieldLookup, discoveredFields, 1);
    Collections.sort(discoveredFields);

    RowMetaInterface rowMeta = new RowMeta();
    for (MongoDbInputData.MongoField m : discoveredFields) {
      ValueMetaInterface vm = new ValueMeta(m.m_fieldName,
          ValueMeta.getType(m.m_kettleType));
      rowMeta.addValueMeta(vm);
    }

    MongoDbInputData data = new MongoDbInputData();
    data.outputRowMeta = rowMeta;
    data.setMongoFields(discoveredFields);
    data.init();
    Variables vars = new Variables();
    Object[] result = data.mongoDocumentToKettle((DBObject) mongoO, vars)[0];
    assertTrue(result != null);
    Object[] expected = { new Long(42), "zzz", "bob", "fred", "george" };

    for (int i = 0; i < rowMeta.size(); i++) {
      assertTrue(result[i] != null);
      assertEquals(expected[i], result[i]);
    }
  }

  @Test
  public void testGetNonExistentField() throws KettleException {
    Object mongoO = JSON.parse(s_testData);
    assertTrue(mongoO instanceof DBObject);

    List<MongoDbInputData.MongoField> discoveredFields = new ArrayList<MongoDbInputData.MongoField>();
    MongoDbInputData.MongoField mm = new MongoDbInputData.MongoField();
    mm.m_fieldName = "test";
    mm.m_fieldPath = "$.iDontExist";
    mm.m_kettleType = "String";
    discoveredFields.add(mm);

    RowMetaInterface rowMeta = new RowMeta();
    for (MongoDbInputData.MongoField m : discoveredFields) {
      ValueMetaInterface vm = new ValueMeta(m.m_fieldName,
          ValueMeta.getType(m.m_kettleType));
      rowMeta.addValueMeta(vm);
    }

    MongoDbInputData data = new MongoDbInputData();
    data.outputRowMeta = rowMeta;
    data.setMongoFields(discoveredFields);
    data.init();
    Variables vars = new Variables();
    Object[] result = data.mongoDocumentToKettle((DBObject) mongoO, vars)[0];

    assertTrue(result != null);
    assertEquals(1, result.length - RowDataUtil.OVER_ALLOCATE_SIZE);
    assertTrue(result[0] == null);
  }

  @Test
  public void testArrayUnwindArrayFieldsOnly() throws KettleException {
    Object mongoO = JSON.parse(s_testData3);
    assertTrue(mongoO instanceof DBObject);

    List<MongoDbInputData.MongoField> fields = new ArrayList<MongoDbInputData.MongoField>();

    MongoDbInputData.MongoField mm = new MongoDbInputData.MongoField();
    mm.m_fieldName = "test";
    mm.m_fieldPath = "$.one.two[*].rec1.f1";
    mm.m_kettleType = "String";

    fields.add(mm);
    RowMetaInterface rowMeta = new RowMeta();
    for (MongoDbInputData.MongoField m : fields) {
      ValueMetaInterface vm = new ValueMeta(m.m_fieldName,
          ValueMeta.getType(m.m_kettleType));
      rowMeta.addValueMeta(vm);
    }

    MongoDbInputData data = new MongoDbInputData();
    data.outputRowMeta = rowMeta;
    data.setMongoFields(fields);
    data.init();
    Variables vars = new Variables();

    Object[][] result = data.mongoDocumentToKettle((DBObject) mongoO, vars);

    assertTrue(result != null);
    assertEquals(2, result.length);

    // should be two rows returned due to the array expansion
    assertTrue(result[0] != null);
    assertTrue(result[1] != null);
    assertEquals("bob", result[0][0]);
    assertEquals("sid", result[1][0]);
  }

  @Test
  public void testArrayUnwindOneArrayExpandFieldAndOneNormalField()
      throws KettleException {
    Object mongoO = JSON.parse(s_testData3);
    assertTrue(mongoO instanceof DBObject);

    List<MongoDbInputData.MongoField> fields = new ArrayList<MongoDbInputData.MongoField>();

    MongoDbInputData.MongoField mm = new MongoDbInputData.MongoField();
    mm.m_fieldName = "test";
    mm.m_fieldPath = "$.one.two[*].rec1.f1";
    mm.m_kettleType = "String";
    fields.add(mm);

    mm = new MongoDbInputData.MongoField();
    mm.m_fieldName = "test2";
    mm.m_fieldPath = "$.name";
    mm.m_kettleType = "String";
    fields.add(mm);

    RowMetaInterface rowMeta = new RowMeta();
    for (MongoDbInputData.MongoField m : fields) {
      ValueMetaInterface vm = new ValueMeta(m.m_fieldName,
          ValueMeta.getType(m.m_kettleType));
      rowMeta.addValueMeta(vm);
    }

    MongoDbInputData data = new MongoDbInputData();
    data.outputRowMeta = rowMeta;
    data.setMongoFields(fields);
    data.init();
    Variables vars = new Variables();

    Object[][] result = data.mongoDocumentToKettle((DBObject) mongoO, vars);

    assertTrue(result != null);
    assertEquals(2, result.length);

    // each row should have two entries
    assertEquals(2 + RowDataUtil.OVER_ALLOCATE_SIZE, result[0].length);

    // should be two rows returned due to the array expansion
    assertTrue(result[0] != null);
    assertTrue(result[1] != null);
    assertEquals("bob", result[0][0]);
    assertEquals("sid", result[1][0]);

    // george should be the name in both rows
    assertEquals("george", result[0][1]);
    assertEquals("george", result[1][1]);
  }

  @Test
  public void testArrayUnwindWithOneExistingAndOneNonExistingField()
      throws KettleException {
    Object mongoO = JSON.parse(s_testData3);
    assertTrue(mongoO instanceof DBObject);

    List<MongoDbInputData.MongoField> fields = new ArrayList<MongoDbInputData.MongoField>();

    MongoDbInputData.MongoField mm = new MongoDbInputData.MongoField();
    mm.m_fieldName = "test";
    mm.m_fieldPath = "$.one.two[*].rec1.f1";
    mm.m_kettleType = "String";
    fields.add(mm);

    mm = new MongoDbInputData.MongoField();
    mm.m_fieldName = "test2";
    mm.m_fieldPath = "$.one.two[*].rec6.nonExistent";
    mm.m_kettleType = "String";
    fields.add(mm);

    RowMetaInterface rowMeta = new RowMeta();
    for (MongoDbInputData.MongoField m : fields) {
      ValueMetaInterface vm = new ValueMeta(m.m_fieldName,
          ValueMeta.getType(m.m_kettleType));
      rowMeta.addValueMeta(vm);
    }

    MongoDbInputData data = new MongoDbInputData();
    data.outputRowMeta = rowMeta;
    data.setMongoFields(fields);
    data.init();
    Variables vars = new Variables();

    Object[][] result = data.mongoDocumentToKettle((DBObject) mongoO, vars);

    assertTrue(result != null);
    assertEquals(2, result.length);

    // should be two rows returned due to the array expansion
    assertTrue(result[0] != null);
    assertTrue(result[1] != null);
    assertEquals("bob", result[0][0]);
    assertEquals("sid", result[1][0]);

    // each row should have two entries
    assertEquals(2 + RowDataUtil.OVER_ALLOCATE_SIZE, result[0].length);

    // this field doesn't exist in the doc structure, so we expect null
    assertTrue(result[0][1] == null);
    assertTrue(result[1][1] == null);
  }

  public static void main(String[] args) {
    MongoDbInputTest test = new MongoDbInputTest();
    try {
      test.testDeterminePaths();
      test.testDeterminePathsWithDisparateTypes();
      test.testGetAllFields();
      test.testGetNonExistentField();
      test.testArrayUnwindArrayFieldsOnly();
      test.testArrayUnwindWithOneExistingAndOneNonExistingField();
      test.testArrayUnwindOneArrayExpandFieldAndOneNormalField();
    } catch (KettleException e) {
      e.printStackTrace();
    }
  }
}
