/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2012 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.trans.steps.mongodbinput;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
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
import org.w3c.dom.Node;

/**
 * Created on 8-apr-2011
 * 
 * @author matt
 * @since 4.2.0-M1
 */
@Step(id = "MongoDbInput", image = "mongodb-input.png", name = "MongoDb Input", description = "Reads from a Mongo DB collection", categoryDescription = "Big Data")
public class MongoDbInputMeta extends BaseStepMeta implements StepMetaInterface {
  protected static Class<?> PKG = MongoDbInputMeta.class; // for i18n purposes,
  // needed by
  // Translator2!!
  // $NON-NLS-1$

  private String hostname;
  private String port;
  private String dbName;
  private String collection;
  private String jsonFieldName;
  private String fields;

  private String authenticationUser;
  private String authenticationPassword;

  private String jsonQuery;

  private boolean m_outputJson = true;

  private List<MongoDbInputData.MongoField> m_fields;

  public MongoDbInputMeta() {
    super(); // allocate BaseStepMeta
  }

  public void setMongoFields(List<MongoDbInputData.MongoField> fields) {
    m_fields = fields;
  }

  public List<MongoDbInputData.MongoField> getMongoFields() {
    return m_fields;
  }

  public void setOutputJson(boolean outputJson) {
    m_outputJson = outputJson;
  }

  public boolean getOutputJson() {
    return m_outputJson;
  }

  public void loadXML(Node stepnode, List<DatabaseMeta> databases,
      Map<String, Counter> counters) throws KettleXMLException {
    readData(stepnode);
  }

  @Override
  public Object clone() {
    MongoDbInputMeta retval = (MongoDbInputMeta) super.clone();
    return retval;
  }

  private void readData(Node stepnode) throws KettleXMLException {
    try {
      hostname = XMLHandler.getTagValue(stepnode, "hostname"); //$NON-NLS-1$ //$NON-NLS-2$
      port = XMLHandler.getTagValue(stepnode, "port"); //$NON-NLS-1$ //$NON-NLS-2$
      dbName = XMLHandler.getTagValue(stepnode, "db_name"); //$NON-NLS-1$
      fields = XMLHandler.getTagValue(stepnode, "fields_name"); //$NON-NLS-1$
      collection = XMLHandler.getTagValue(stepnode, "collection"); //$NON-NLS-1$
      jsonFieldName = XMLHandler.getTagValue(stepnode, "json_field_name"); //$NON-NLS-1$
      jsonQuery = XMLHandler.getTagValue(stepnode, "json_query"); //$NON-NLS-1$
      authenticationUser = XMLHandler.getTagValue(stepnode, "auth_user"); //$NON-NLS-1$
      authenticationPassword = Encr
          .decryptPasswordOptionallyEncrypted(XMLHandler.getTagValue(stepnode,
              "auth_password")); //$NON-NLS-1$

      m_outputJson = true; // default to true for backwards compatibility
      String outputJson = XMLHandler.getTagValue(stepnode, "output_json");
      if (!Const.isEmpty(outputJson)) {
        m_outputJson = outputJson.equalsIgnoreCase("Y");
      }

      Node fields = XMLHandler.getSubNode(stepnode, "mongo_fields");
      if (fields != null && XMLHandler.countNodes(fields, "mongo_field") > 0) {
        int nrfields = XMLHandler.countNodes(fields, "mongo_field");

        m_fields = new ArrayList<MongoDbInputData.MongoField>();
        for (int i = 0; i < nrfields; i++) {
          Node fieldNode = XMLHandler.getSubNodeByNr(fields, "mongo_field", i);

          MongoDbInputData.MongoField newField = new MongoDbInputData.MongoField();
          newField.m_fieldName = XMLHandler
              .getTagValue(fieldNode, "field_name");
          newField.m_fieldPath = XMLHandler
              .getTagValue(fieldNode, "field_path");
          newField.m_kettleType = XMLHandler.getTagValue(fieldNode,
              "field_type");
          String indexedVals = XMLHandler
              .getTagValue(fieldNode, "indexed_vals");
          if (indexedVals != null && indexedVals.length() > 0) {
            newField.m_indexedVals = MongoDbInputData
                .indexedValsList(indexedVals);
          }

          m_fields.add(newField);
        }
      }
    } catch (Exception e) {
      throw new KettleXMLException(BaseMessages.getString(PKG,
          "MongoDbInputMeta.Exception.UnableToLoadStepInfo"), e); //$NON-NLS-1$
    }
  }

  public void setDefault() {
    hostname = "localhost"; //$NON-NLS-1$
    port = "27017"; //$NON-NLS-1$
    dbName = "db"; //$NON-NLS-1$
    collection = "collection"; //$NON-NLS-1$
    jsonFieldName = "json"; //$NON-NLS-1$
  }

  @Override
  public void getFields(RowMetaInterface rowMeta, String origin,
      RowMetaInterface[] info, StepMeta nextStep, VariableSpace space)
      throws KettleStepException {

    if (m_outputJson || m_fields == null || m_fields.size() == 0) {
      ValueMetaInterface jsonValueMeta = new ValueMeta(jsonFieldName,
          ValueMetaInterface.TYPE_STRING);
      jsonValueMeta.setOrigin(origin);
      rowMeta.addValueMeta(jsonValueMeta);
    } else {
      for (MongoDbInputData.MongoField f : m_fields) {
        ValueMetaInterface vm = new ValueMeta();
        vm.setName(f.m_fieldName);
        vm.setOrigin(origin);
        vm.setType(ValueMeta.getType(f.m_kettleType));
        if (f.m_indexedVals != null) {
          vm.setIndex(f.m_indexedVals.toArray()); // indexed values
        }
        rowMeta.addValueMeta(vm);
      }
    }
  }

  @Override
  public String getXML() {
    StringBuffer retval = new StringBuffer(300);

    retval.append("    ").append(XMLHandler.addTagValue("hostname", hostname)); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append("    ").append(XMLHandler.addTagValue("port", port)); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append("    ").append(XMLHandler.addTagValue("db_name", dbName)); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append("    ").append(XMLHandler.addTagValue("fields_name", fields)); //$NON-NLS-1$ //$NON-NLS-2$
    retval
        .append("    ").append(XMLHandler.addTagValue("collection", collection)); //$NON-NLS-1$ //$NON-NLS-2$
    retval
        .append("    ").append(XMLHandler.addTagValue("json_field_name", jsonFieldName)); //$NON-NLS-1$ //$NON-NLS-2$
    retval
        .append("    ").append(XMLHandler.addTagValue("json_query", jsonQuery)); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append("    ").append(
        XMLHandler.addTagValue("auth_user", authenticationUser));
    retval.append("    ").append(
        XMLHandler.addTagValue("auth_password",
            Encr.encryptPasswordIfNotUsingVariables(authenticationPassword)));
    retval.append("    ").append(
        XMLHandler.addTagValue("output_json", m_outputJson));

    if (m_fields != null && m_fields.size() > 0) {
      retval.append("\n    ").append(XMLHandler.openTag("mongo_fields"));

      for (MongoDbInputData.MongoField f : m_fields) {
        retval.append("\n      ").append(XMLHandler.openTag("mongo_field"));

        retval.append("\n        ").append(
            XMLHandler.addTagValue("field_name", f.m_fieldName));
        retval.append("\n        ").append(
            XMLHandler.addTagValue("field_path", f.m_fieldPath));
        retval.append("\n        ").append(
            XMLHandler.addTagValue("field_type", f.m_kettleType));
        if (f.m_indexedVals != null && f.m_indexedVals.size() > 0) {
          retval.append("\n        ").append(
              XMLHandler.addTagValue("indexed_vals",
                  MongoDbInputData.indexedValsList(f.m_indexedVals)));
        }
        retval.append("\n      ").append(XMLHandler.closeTag("mongo_field"));
      }

      retval.append("\n    ").append(XMLHandler.closeTag("mongo_fields"));
    }

    return retval.toString();
  }

  public void readRep(Repository rep, ObjectId id_step,
      List<DatabaseMeta> databases, Map<String, Counter> counters)
      throws KettleException {
    try {
      hostname = rep.getStepAttributeString(id_step, "hostname"); //$NON-NLS-1$
      port = rep.getStepAttributeString(id_step, "port"); //$NON-NLS-1$
      dbName = rep.getStepAttributeString(id_step, "db_name"); //$NON-NLS-1$
      fields = rep.getStepAttributeString(id_step, "fields_name"); //$NON-NLS-1$
      collection = rep.getStepAttributeString(id_step, "collection"); //$NON-NLS-1$
      jsonFieldName = rep.getStepAttributeString(id_step, "json_field_name"); //$NON-NLS-1$
      jsonQuery = rep.getStepAttributeString(id_step, "json_query"); //$NON-NLS-1$

      authenticationUser = rep.getStepAttributeString(id_step, "auth_user");
      authenticationPassword = Encr.decryptPasswordOptionallyEncrypted(rep
          .getStepAttributeString(id_step, "auth_password"));

      m_outputJson = rep.getStepAttributeBoolean(id_step, 0, "output_json");

      int nrfields = rep.countNrStepAttributes(id_step, "field_name");
      if (nrfields > 0) {
        m_fields = new ArrayList<MongoDbInputData.MongoField>();

        for (int i = 0; i < nrfields; i++) {
          MongoDbInputData.MongoField newField = new MongoDbInputData.MongoField();

          newField.m_fieldName = rep.getStepAttributeString(id_step, i,
              "field_name");
          newField.m_fieldPath = rep.getStepAttributeString(id_step, i,
              "field_path");
          newField.m_kettleType = rep.getStepAttributeString(id_step, i,
              "field_type");
          String indexedVals = rep.getStepAttributeString(id_step, i,
              "indexed_vals");
          if (indexedVals != null && indexedVals.length() > 0) {
            newField.m_indexedVals = MongoDbInputData
                .indexedValsList(indexedVals);
          }

          m_fields.add(newField);
        }
      }
    } catch (Exception e) {
      throw new KettleException(BaseMessages.getString(PKG,
          "MongoDbInputMeta.Exception.UnexpectedErrorWhileReadingStepInfo"), e); //$NON-NLS-1$
    }
  }

  public void saveRep(Repository rep, ObjectId id_transformation,
      ObjectId id_step) throws KettleException {
    try {
      rep.saveStepAttribute(id_transformation, id_step, "hostname", hostname); //$NON-NLS-1$
      rep.saveStepAttribute(id_transformation, id_step, "port", port); //$NON-NLS-1$
      rep.saveStepAttribute(id_transformation, id_step, "db_name", dbName); //$NON-NLS-1$
      rep.saveStepAttribute(id_transformation, id_step, "fields_name", fields); //$NON-NLS-1$
      rep.saveStepAttribute(id_transformation, id_step,
          "collection", collection); //$NON-NLS-1$
      rep.saveStepAttribute(id_transformation, id_step,
          "json_field_name", jsonFieldName); //$NON-NLS-1$
      rep.saveStepAttribute(id_transformation, id_step, "json_query", jsonQuery); //$NON-NLS-1$

      rep.saveStepAttribute(id_transformation, id_step, "auth_user",
          authenticationUser);
      rep.saveStepAttribute(id_transformation, id_step, "auth_password",
          Encr.encryptPasswordIfNotUsingVariables(authenticationPassword));
      rep.saveStepAttribute(id_transformation, id_step, 0, "output_json",
          m_outputJson);

      if (m_fields != null && m_fields.size() > 0) {
        for (int i = 0; i < m_fields.size(); i++) {
          MongoDbInputData.MongoField f = m_fields.get(i);

          rep.saveStepAttribute(id_transformation, id_step, i, "field_name",
              f.m_fieldName);
          rep.saveStepAttribute(id_transformation, id_step, i, "field_path",
              f.m_fieldPath);
          rep.saveStepAttribute(id_transformation, id_step, i, "field_type",
              f.m_kettleType);
          if (f.m_indexedVals != null && f.m_indexedVals.size() > 0) {
            String indexedVals = MongoDbInputData
                .indexedValsList(f.m_indexedVals);

            rep.saveStepAttribute(id_transformation, id_step, i,
                "indexed_vals", indexedVals);
          }
        }
      }
    } catch (KettleException e) {
      throw new KettleException(BaseMessages.getString(PKG,
          "MongoDbInputMeta.Exception.UnableToSaveStepInfo") + id_step, e); //$NON-NLS-1$
    }
  }

  public StepInterface getStep(StepMeta stepMeta,
      StepDataInterface stepDataInterface, int cnr, TransMeta tr, Trans trans) {
    return new MongoDbInput(stepMeta, stepDataInterface, cnr, tr, trans);
  }

  public StepDataInterface getStepData() {
    return new MongoDbInputData();
  }

  public void check(List<CheckResultInterface> remarks, TransMeta transMeta,
      StepMeta stepMeta, RowMetaInterface prev, String[] input,
      String[] output, RowMetaInterface info) {
    // TODO add checks
  }

  /**
   * @return the hostname
   */
  public String getHostname() {
    return hostname;
  }

  /**
   * @param hostname the hostname to set
   */
  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  /**
   * @return the port
   */
  public String getPort() {
    return port;
  }

  /**
   * @param port the port to set
   */
  public void setPort(String port) {
    this.port = port;
  }

  /**
   * @return the dbName
   */
  public String getDbName() {
    return dbName;
  }

  /**
   * @param dbName the dbName to set
   */
  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  /**
   * @return the fields
   */
  public String getFieldsName() {
    return fields;
  }

  /**
   * @param dbName the dbName to set
   */
  public void setFieldsName(String fields) {
    this.fields = fields;
  }

  /**
   * @return the collection
   */
  public String getCollection() {
    return collection;
  }

  /**
   * @param collection the collection to set
   */
  public void setCollection(String collection) {
    this.collection = collection;
  }

  /**
   * @return the jsonFieldName
   */
  public String getJsonFieldName() {
    return jsonFieldName;
  }

  /**
   * @param jsonFieldName the jsonFieldName to set
   */
  public void setJsonFieldName(String jsonFieldName) {
    this.jsonFieldName = jsonFieldName;
  }

  /**
   * @return the authenticationUser
   */
  public String getAuthenticationUser() {
    return authenticationUser;
  }

  /**
   * @param authenticationUser the authenticationUser to set
   */
  public void setAuthenticationUser(String authenticationUser) {
    this.authenticationUser = authenticationUser;
  }

  /**
   * @return the authenticationPassword
   */
  public String getAuthenticationPassword() {
    return authenticationPassword;
  }

  /**
   * @param authenticationPassword the authenticationPassword to set
   */
  public void setAuthenticationPassword(String authenticationPassword) {
    this.authenticationPassword = authenticationPassword;
  }

  /**
   * @return the jsonQuery
   */
  public String getJsonQuery() {
    return jsonQuery;
  }

  /**
   * @param jsonQuery the jsonQuery to set
   */
  public void setJsonQuery(String jsonQuery) {
    this.jsonQuery = jsonQuery;
  }

}
