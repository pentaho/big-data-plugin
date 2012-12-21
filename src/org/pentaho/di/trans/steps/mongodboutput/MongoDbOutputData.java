/*******************************************************************************
 *
 * Pentaho Big Data
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

package org.pentaho.di.trans.steps.mongodboutput;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;

/**
 * Data class for the MongoDbOutput step
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 */
public class MongoDbOutputData extends BaseStepData implements
    StepDataInterface {

  private static Class<?> PKG = MongoDbOutputMeta.class;

  public static final int MONGO_DEFAULT_PORT = 27017;

  /** Enum for the type of the top level object of the document structure */
  public static enum MongoTopLevel {
    RECORD, ARRAY, INCONSISTENT;
  }

  /** The output row format */
  protected RowMetaInterface m_outputRowMeta;

  /** Main entry point to the mongo driver */
  protected MongoClient m_mongo;

  /** DB object for the user-selected database */
  protected DB m_db;

  /** Collection object for the user-specified document collection */
  protected DBCollection m_collection;

  protected List<MongoDbOutputMeta.MongoField> m_userFields;

  /**
   * Map for grouping together $set operations that involve setting complex
   * array-based objects. Key = dot path to array name; value = DBObject
   * specifying array to set to
   */
  protected Map<String, List<MongoDbOutputMeta.MongoField>> m_setComplexArrays = new HashMap<String, List<MongoDbOutputMeta.MongoField>>();

  /**
   * Map for grouping together $push operations that involve complex objects.
   * Use [] to indicate the start of the complex object to push. Key - dot path
   * to the array name to push to; value - DBObject specifying the complex
   * object to push
   */
  protected Map<String, List<MongoDbOutputMeta.MongoField>> m_pushComplexStructures = new HashMap<String, List<MongoDbOutputMeta.MongoField>>();

  /** all other modifier updates that involve primitive leaf fields */
  protected Map<String, Object[]> m_primitiveLeafModifiers = new LinkedHashMap<String, Object[]>();

  /**
   * Set the field paths to use for creating the document structure
   * 
   * @param fields the field paths to use
   */
  public void setMongoFields(List<MongoDbOutputMeta.MongoField> fields) {
    // copy this list
    m_userFields = new ArrayList<MongoDbOutputMeta.MongoField>();

    for (MongoDbOutputMeta.MongoField f : fields) {
      m_userFields.add(f.copy());
    }
  }

  /**
   * Initialize field paths
   * 
   * @param vars variables to use
   * @throws KettleException if a problem occurs
   */
  public void init(VariableSpace vars) throws KettleException {
    if (m_userFields != null) {
      for (MongoDbOutputMeta.MongoField f : m_userFields) {
        f.init(vars);
      }
    }
  }

  /**
   * Utility method to configure Mongo connection options based on parameters
   * supplied in the meta data
   * 
   * @param optsBuilder an options builder
   * @param meta the step meta data
   * @param vars variables to use
   * @throws KettleException if a problem occurs
   */
  public static void configureConnectionOptions(
      MongoClientOptions.Builder optsBuilder, MongoDbOutputMeta meta,
      VariableSpace vars) throws KettleException {

    // connection timeout
    if (!Const.isEmpty(meta.getConnectTimeout())) {
      String connS = meta.getConnectTimeout();
      connS = vars.environmentSubstitute(connS);
      try {
        int connTimeout = Integer.parseInt(connS);
        if (connTimeout > 0) {
          optsBuilder.connectTimeout(connTimeout);
        }
      } catch (NumberFormatException n) {
        throw new KettleException(n);
      }
    }

    // socket timeout
    if (!Const.isEmpty(meta.getSocketTimeout())) {
      String sockS = meta.getSocketTimeout();
      sockS = vars.environmentSubstitute(sockS);
      try {
        int sockTimeout = Integer.parseInt(sockS);
        if (sockTimeout > 0) {
          optsBuilder.socketTimeout(sockTimeout);
        }
      } catch (NumberFormatException n) {
        throw new KettleException(n);
      }
    }

    // read preference
    if (!Const.isEmpty(meta.getReadPreference())) {
      String rp = meta.getReadPreference();
      rp = vars.environmentSubstitute(rp);

      if (rp.equalsIgnoreCase("Primary")) {
        optsBuilder.readPreference(ReadPreference.primary());
      } else if (rp.equalsIgnoreCase("Primary preferred")) {
        optsBuilder.readPreference(ReadPreference.primaryPreferred());
      } else if (rp.equalsIgnoreCase("Secondary")) {
        optsBuilder.readPreference(ReadPreference.secondary());
      } else if (rp.equalsIgnoreCase("Secondary preferred")) {
        optsBuilder.readPreference(ReadPreference.secondaryPreferred());
      } else if (rp.equalsIgnoreCase("Nearest")) {
        optsBuilder.readPreference(ReadPreference.nearest());
      }
    }

    // write concern
    String writeConcern = meta.getWriteConcern();
    String wTimeout = meta.getWTimeout();
    boolean journaled = meta.getJournal();
    WriteConcern concern = null;

    if (Const.isEmpty(writeConcern) && Const.isEmpty(wTimeout) && !journaled) {
      concern = new WriteConcern(); // all defaults - timeout 0, journal =
                                    // false, w = 1
    } else {
      int wt = 0;
      if (!Const.isEmpty(wTimeout)) {
        try {
          wt = Integer.parseInt(wTimeout);
        } catch (NumberFormatException n) {
          throw new KettleException(n);
        }

        if (!Const.isEmpty(writeConcern)) {
          // try parsing as a number first
          try {
            int wc = Integer.parseInt(writeConcern);
            concern = new WriteConcern(wc, wt, false, journaled);
          } catch (NumberFormatException n) {
            // assume its a valid string - e.g. "majority" or a list of tags
            concern = new WriteConcern(writeConcern, wt, false, journaled);
          }
        } else {
          concern = new WriteConcern(1, wt, false, journaled);
        }
      }
    }
    optsBuilder.writeConcern(concern);
  }

  /**
   * Create a connection to a Mongo server based on parameters supplied in the
   * step meta data
   * 
   * @param meta the step meta data
   * @param vars variables to use
   * @return a configured MongoClient object
   * @throws KettleException if a problem occurs
   */
  public static MongoClient connect(MongoDbOutputMeta meta, VariableSpace vars)
      throws KettleException {

    String hostsPorts = meta.getHostnames();
    String singlePort = meta.getPort();
    hostsPorts = vars.environmentSubstitute(hostsPorts);
    singlePort = vars.environmentSubstitute(singlePort);
    int singlePortI = -1;

    try {
      singlePortI = Integer.parseInt(singlePort);
    } catch (NumberFormatException n) {
      // don't complain
    }

    if (Const.isEmpty(hostsPorts)) {
      throw new KettleException("Empty hosts string");
    }

    List<ServerAddress> repSet = new ArrayList<ServerAddress>();

    String[] parts = hostsPorts.trim().split(",");
    for (String part : parts) {
      // host:port?
      int port = singlePortI != -1 ? singlePortI : MONGO_DEFAULT_PORT;
      String[] hp = part.split(":");
      if (hp.length > 2) {
        throw new KettleException("Malformed host spec: " + part);
      }

      String host = hp[0];
      if (hp.length == 2) {
        // non-default port
        try {
          port = Integer.parseInt(hp[1].trim());
        } catch (NumberFormatException n) {
          throw new KettleException("Unable to parse port number: " + hp[1]);
        }
      }

      try {
        ServerAddress s = new ServerAddress(host, port);
        repSet.add(s);
      } catch (UnknownHostException u) {
        throw new KettleException(u);
      }
    }

    MongoClientOptions.Builder mongoOptsBuilder = new MongoClientOptions.Builder();
    if (meta != null) {
      configureConnectionOptions(mongoOptsBuilder, meta, vars);
    }
    MongoClientOptions opts = mongoOptsBuilder.build();
    try {
      return (repSet.size() > 1 ? new MongoClient(repSet, opts) : (repSet
          .size() == 1 ? new MongoClient(repSet.get(0), opts)
          : new MongoClient(new ServerAddress("localhost"), opts)));
    } catch (UnknownHostException u) {
      throw new KettleException(u);
    }
  }

  /**
   * Disconnect from Mongo
   */
  public static void disconnect(MongoClient mongo) {
    if (mongo != null) {
      mongo.close();
    }
  }

  /**
   * Get the current connection or null if not connected
   * 
   * @return the connection or null
   */
  public MongoClient getConnection() {
    return m_mongo;
  }

  /**
   * Set the current connection
   * 
   * @param mongo the connection to use
   */
  public void setConnection(MongoClient mongo) {
    m_mongo = mongo;
  }

  /**
   * Set the database to use
   * 
   * @param db the DB object
   */
  public void setDB(DB db) {
    m_db = db;
  }

  /**
   * Get the database
   * 
   * @return the database
   */
  public DB getDB() {
    return m_db;
  }

  /**
   * Create a collection in the current database
   * 
   * @param collectionName the name of the collection to create
   * @throws Exception if a problem occurs
   */
  public void createCollection(String collectionName) throws Exception {
    if (m_db == null) {
      throw new Exception(BaseMessages.getString(PKG,
          "MongoDbOutput.Messages.Error.NoDatabaseSet"));
    }

    m_db.createCollection(collectionName, null);
  }

  /**
   * Set the collection to use
   * 
   * @param col the collection to use
   */
  public void setCollection(DBCollection col) {
    m_collection = col;
  }

  /**
   * Get the collection in use
   * 
   * @return the collection in use
   */
  public DBCollection getCollection() {
    return m_collection;
  }

  /**
   * Set the output row format
   * 
   * @param outM the output row format
   */
  public void setOutputRowMeta(RowMetaInterface outM) {
    m_outputRowMeta = outM;
  }

  /**
   * Get the output row format
   * 
   * @return the output row format
   */
  public RowMetaInterface getOutputRowMeta() {
    return m_outputRowMeta;
  }

  /**
   * Apply the supplied index operations to the collection. Indexes can be
   * defined on one or more fields in the document. Operation is either create
   * or drop.
   * 
   * @param indexes a list of index operations
   * @param log the logging object
   * @param truncate true if the collection was truncated in the current
   *          execution - in this case drop operations are not necessary
   * @throws MongoException if something goes wrong
   */
  public void applyIndexes(List<MongoDbOutputMeta.MongoIndex> indexes,
      LogChannelInterface log, boolean truncate) throws MongoException {

    for (MongoDbOutputMeta.MongoIndex index : indexes) {
      String[] indexParts = index.m_pathToFields.split(",");
      BasicDBObject mongoIndex = new BasicDBObject();
      for (String indexKey : indexParts) {
        String[] nameAndDirection = indexKey.split(":");
        int direction = 1;
        if (nameAndDirection.length == 2) {
          direction = Integer.parseInt(nameAndDirection[1].trim());
        }
        String name = nameAndDirection[0];

        // strip off brackets to get actual object name if terminal object
        // is an array
        if (name.indexOf('[') > 0) {
          name = name.substring(name.indexOf('[') + 1, name.length());
        }

        mongoIndex.put(name, direction);
      }

      if (index.m_drop) {
        if (truncate) {
          log.logBasic(BaseMessages.getString(PKG,
              "MongoDbOutput.Messages.TruncateBeforeInsert", index));
        } else {
          m_collection.dropIndex(mongoIndex);
        }
        log.logBasic(BaseMessages.getString(PKG,
            "MongoDbOutput.Messages.DropIndex", index));
      } else {
        BasicDBObject options = new BasicDBObject();

        // create indexes in the background
        options.put("background", true);
        options.put("unique", index.m_unique);
        options.put("sparse", index.m_sparse);
        m_collection.createIndex(mongoIndex);
        log.logBasic(BaseMessages.getString(PKG,
            "MongoDbOutput.Messages.CreateIndex", index));
      }
    }
  }

  /**
   * Get an object that encapsulates the fields and modifier operations to use
   * for a modifier update.
   * 
   * NOTE: that with modifier upserts the query conditions get created if the
   * record does not exist (i.e. insert). This is different than straight non-
   * modifier upsert where the query conditions just locate a matching record
   * (if any) and then a complete object replacement is done. So for standard
   * upsert it is necessary to duplicate the query condition paths in order for
   * these fields to be in the object that is inserted/updated.
   * 
   * This also means that certain modifier upserts are not possible in the case
   * of insert. E.g. here we are wanting to test if the field "f1" in record
   * "rec1" in the first element of array "two" is set to "george". If so, then
   * we want to push a new record to the end of the array; otherwise create a
   * new document with the array containing just the new record:
   * <p>
   * 
   * <pre>
   * db.collection.update({ "one.two.0.rec1.f1" : "george"}, 
   * { "$push" : { "one.two" : { "rec1" : { "f1" : "bob" , "f2" : "fred"}}}}, 
   * true)
   * </pre>
   * 
   * This does not work and results in a
   * "Cannot apply $push/$pushAll modifier to non-array" error if there is no
   * match (i.e. insert condition). This is because the query conditions get
   * created as well as the modifier opps and, furthermore, they get created
   * first. Since mongo doesn't know whether ".0." indicates an array index or a
   * field name it defaults to creating a field with name "0". This means that
   * "one.two" gets created as a record (not an array) before the $push
   * operation is executed. Hence the error.
   * 
   * @param fieldDefs the list of document field definitions
   * @param inputMeta the input row format
   * @param row the current incoming row
   * @param vars environment variables
   * @param topLevelStructure the top level structure of the document
   * @return a DBObject encapsulating the update to make
   * @throws KettleException if a problem occurs
   */
  protected DBObject getModifierUpdateObject(
      List<MongoDbOutputMeta.MongoField> fieldDefs, RowMetaInterface inputMeta,
      Object[] row, VariableSpace vars, MongoTopLevel topLevelStructure)
      throws KettleException {

    boolean haveUpdateFields = false;
    boolean hasNonNullUpdateValues = false;

    // main update object, keyed by $ operator
    BasicDBObject updateObject = new BasicDBObject();

    m_setComplexArrays.clear();
    m_primitiveLeafModifiers.clear();
    m_pushComplexStructures.clear();

    // do we need to determine whether this will be an insert or an update?
    boolean checkForMatch = false;
    for (MongoDbOutputMeta.MongoField field : fieldDefs) {
      if (!field.m_updateMatchField
          && (field.m_modifierOperationApplyPolicy.equals("Insert") || field.m_modifierOperationApplyPolicy
              .equals("Update"))) {
        checkForMatch = true;
        break;
      }
    }

    boolean isUpdate = false;
    if (checkForMatch) {
      DBObject query = getQueryObject(fieldDefs, inputMeta, row, vars,
          topLevelStructure);

      DBCursor cursor = getCollection().find(query).limit(1);
      if (cursor.hasNext()) {
        isUpdate = true;
      }
    }

    for (MongoDbOutputMeta.MongoField field : fieldDefs) {
      // skip query match fields
      if (field.m_updateMatchField) {
        continue;
      }

      String modifierUpdateOpp = vars
          .environmentSubstitute(field.m_modifierUpdateOperation);

      if (!Const.isEmpty(modifierUpdateOpp) && !modifierUpdateOpp.equals("N/A")) {
        if (checkForMatch) {
          if (isUpdate && field.m_modifierOperationApplyPolicy.equals("Insert")) {
            continue; // don't apply this opp
          }

          if (!isUpdate
              && field.m_modifierOperationApplyPolicy.equals("Update")) {
            continue; // don't apply this opp
          }
        }

        haveUpdateFields = true;

        String incomingFieldName = vars
            .environmentSubstitute(field.m_incomingFieldName);
        int index = inputMeta.indexOfValue(incomingFieldName);
        ValueMetaInterface vm = inputMeta.getValueMeta(index);

        if (!vm.isNull(row[index])) {
          hasNonNullUpdateValues = true;

          // modifier update objects have fields using "dot" notation to reach
          // into embedded documents
          String mongoPath = (field.m_mongoDocPath != null) ? field.m_mongoDocPath
              : "";
          String path = vars.environmentSubstitute(mongoPath);

          if (path.endsWith("]") && modifierUpdateOpp.equals("$push")
              && !field.m_useIncomingFieldNameAsMongoFieldName) {

            // strip off the brackets as push appends to the end of the named
            // array
            path = path.substring(0, path.indexOf('['));
          }

          boolean hasPath = !Const.isEmpty(path);
          path += ((field.m_useIncomingFieldNameAsMongoFieldName) ? (hasPath ? "."
              + incomingFieldName
              : incomingFieldName)
              : "");

          // check for array creation
          if (modifierUpdateOpp.equals("$set") && path.indexOf('[') > 0) {
            String arrayPath = path.substring(0, path.indexOf('['));
            String arraySpec = path.substring(path.indexOf('['), path.length());
            MongoDbOutputMeta.MongoField a = new MongoDbOutputMeta.MongoField();
            a.m_incomingFieldName = field.m_incomingFieldName;
            a.m_mongoDocPath = arraySpec;
            // incoming field name has already been appended (if necessary)
            a.m_useIncomingFieldNameAsMongoFieldName = false;
            a.init(vars);
            List<MongoDbOutputMeta.MongoField> fds = m_setComplexArrays
                .get(arrayPath);
            if (fds == null) {
              fds = new ArrayList<MongoDbOutputMeta.MongoField>();
              m_setComplexArrays.put(arrayPath, fds);
            }
            fds.add(a);
          } else if (modifierUpdateOpp.equals("$push") && path.indexOf('[') > 0) {
            // we ignore any index that might have been specified as $push
            // always appends to the end of the array.
            String arrayPath = path.substring(0, path.indexOf('['));
            String structureToPush = path.substring(path.indexOf(']') + 1,
                path.length());

            // check to see if we're pushing a record at this point in the path
            // or another array...
            if (structureToPush.charAt(0) == '.') {
              // skip the dot
              structureToPush = structureToPush.substring(1,
                  structureToPush.length());
            }

            MongoDbOutputMeta.MongoField a = new MongoDbOutputMeta.MongoField();
            a.m_incomingFieldName = field.m_incomingFieldName;
            a.m_mongoDocPath = structureToPush;
            // incoming field name has already been appended (if necessary)
            a.m_useIncomingFieldNameAsMongoFieldName = false;
            a.init(vars);
            List<MongoDbOutputMeta.MongoField> fds = m_pushComplexStructures
                .get(arrayPath);
            if (fds == null) {
              fds = new ArrayList<MongoDbOutputMeta.MongoField>();
              m_pushComplexStructures.put(arrayPath, fds);
            }
            fds.add(a);
          } else {
            Object[] params = new Object[2];
            params[0] = modifierUpdateOpp;
            params[1] = new Integer(index);
            m_primitiveLeafModifiers.put(path, params);
          }
        }
      }
    }

    // do the array $sets
    for (String path : m_setComplexArrays.keySet()) {
      List<MongoDbOutputMeta.MongoField> fds = m_setComplexArrays.get(path);
      DBObject valueToSet = kettleRowToMongo(fds, inputMeta, row, vars,
          MongoTopLevel.ARRAY);

      DBObject fieldsToUpdateWithValues = null;

      if (updateObject.get("$set") != null) {
        // if we have some field(s) already associated with this type of
        // modifier
        // operation then just add to them
        fieldsToUpdateWithValues = (DBObject) updateObject.get("$set");
      } else {
        // otherwise create a new DBObject for this modifier operation
        fieldsToUpdateWithValues = new BasicDBObject();
      }

      fieldsToUpdateWithValues.put(path, valueToSet);
      updateObject.put("$set", fieldsToUpdateWithValues);
    }

    // now do the $push complex
    for (String path : m_pushComplexStructures.keySet()) {
      List<MongoDbOutputMeta.MongoField> fds = m_pushComplexStructures
          .get(path);

      // check our top-level structure
      MongoTopLevel topLevel = MongoTopLevel.RECORD;
      if (fds.get(0).m_mongoDocPath.charAt(0) == '[') {
        topLevel = MongoTopLevel.RECORD;
      }

      DBObject valueToSet = kettleRowToMongo(fds, inputMeta, row, vars,
          topLevel);

      DBObject fieldsToUpdateWithValues = null;

      if (updateObject.get("$push") != null) {
        // if we have some field(s) already associated with this type of
        // modifier
        // operation then just add to them
        fieldsToUpdateWithValues = (DBObject) updateObject.get("$push");
      } else {
        // otherwise create a new DBObject for this modifier operation
        fieldsToUpdateWithValues = new BasicDBObject();
      }

      fieldsToUpdateWithValues.put(path, valueToSet);
      updateObject.put("$push", fieldsToUpdateWithValues);
    }

    // do the modifiers that involve primitive field values
    for (String path : m_primitiveLeafModifiers.keySet()) {
      Object[] params = m_primitiveLeafModifiers.get(path);
      String modifierUpdateOpp = params[0].toString();
      int index = ((Integer) params[1]).intValue();
      ValueMetaInterface vm = inputMeta.getValueMeta(index);

      DBObject fieldsToUpdateWithValues = null;

      if (updateObject.get(modifierUpdateOpp) != null) {
        // if we have some field(s) already associated with this type of
        // modifier
        // operation then just add to them
        fieldsToUpdateWithValues = (DBObject) updateObject
            .get(modifierUpdateOpp);
      } else {
        // otherwise create a new DBObject for this modifier operation
        fieldsToUpdateWithValues = new BasicDBObject();
      }
      setMongoValueFromKettleValue(fieldsToUpdateWithValues, path, vm,
          row[index]);

      updateObject.put(modifierUpdateOpp, fieldsToUpdateWithValues);
    }

    if (!haveUpdateFields) {
      throw new KettleException(
          BaseMessages
              .getString(PKG,
                  "MongoDbOutput.Messages.Error.NoFieldsToUpdateSpecifiedForModifierOpp"));
    }

    if (!hasNonNullUpdateValues) {
      return null;
    }

    return updateObject;
  }

  /**
   * Get an object that encapsulates the query to make for an update/upsert
   * operation
   * 
   * @param fieldDefs the list of document field definitions
   * @param inputMeta the input row format
   * @param row the current incoming row
   * @param vars environment variables
   * @return a DBObject encapsulating the query
   * @throws KettleException if something goes wrong
   */
  protected static DBObject getQueryObject(
      List<MongoDbOutputMeta.MongoField> fieldDefs, RowMetaInterface inputMeta,
      Object[] row, VariableSpace vars, MongoTopLevel topLevelStructure)
      throws KettleException {
    BasicDBObject query = new BasicDBObject();

    boolean haveMatchFields = false;
    boolean hasNonNullMatchValues = false;

    for (MongoDbOutputMeta.MongoField field : fieldDefs) {
      if (field.m_updateMatchField) {
        haveMatchFields = true;
        String incomingFieldName = vars
            .environmentSubstitute(field.m_incomingFieldName);
        int index = inputMeta.indexOfValue(incomingFieldName);
        ValueMetaInterface vm = inputMeta.getValueMeta(index);

        // ignore null fields
        if (vm.isNull(row[index])) {
          continue;
        }

        hasNonNullMatchValues = true;

        // query objects have fields using "dot" notation to reach into embedded
        // documents
        String mongoPath = (field.m_mongoDocPath != null) ? field.m_mongoDocPath
            : "";
        String path = vars.environmentSubstitute(mongoPath);
        boolean hasPath = !Const.isEmpty(path);
        path += ((field.m_useIncomingFieldNameAsMongoFieldName) ? (hasPath ? "."
            + incomingFieldName
            : incomingFieldName)
            : "");

        // post process arrays to fit the dot notation (if not already done
        // by the user)
        if (path.indexOf('[') > 0) {
          path = path.replace("[", ".").replace("]", "");
        }

        setMongoValueFromKettleValue(query, path, vm, row[index]);
      }
    }

    if (!haveMatchFields) {
      throw new KettleException(BaseMessages.getString(PKG,
          "MongoDbOutput.Messages.Error.NoFieldsToUpdateSpecifiedForMatch"));
    }

    if (!hasNonNullMatchValues) {
      // indicates that we don't have anything to match with with respect to
      // this row
      return null;
    }

    return query;
  }

  /**
   * Converts a kettle row to a Mongo Object for inserting/updating
   * 
   * @param fieldDefs the document field definitions
   * @param inputMeta the incoming row format
   * @param row the current incoming row
   * @param vars environment variables
   * @param topLevelStructure the top level structure of the Mongo document
   * @return a DBObject encapsulating the document to insert/upsert or null if
   *         there are no non-null incoming fields
   * @throws KettleException if a problem occurs
   */
  protected static DBObject kettleRowToMongo(
      List<MongoDbOutputMeta.MongoField> fieldDefs, RowMetaInterface inputMeta,
      Object[] row, VariableSpace vars, MongoTopLevel topLevelStructure)
      throws KettleException {

    DBObject root = null;
    if (topLevelStructure == MongoTopLevel.RECORD) {
      root = new BasicDBObject();
    } else if (topLevelStructure == MongoTopLevel.ARRAY) {
      root = new BasicDBList();
    }

    if (vars == null) {
      vars = new Variables();
    }

    boolean haveNonNullFields = false;
    for (MongoDbOutputMeta.MongoField field : fieldDefs) {
      if (!field.m_updateMatchField) {
        DBObject current = root;

        field.reset();
        List<String> pathParts = field.m_tempPathList;
        String incomingFieldName = vars
            .environmentSubstitute(field.m_incomingFieldName);
        int index = inputMeta.indexOfValue(incomingFieldName);
        ValueMetaInterface vm = inputMeta.getValueMeta(index);

        Object lookup = getPathElementName(pathParts, current,
            field.m_useIncomingFieldNameAsMongoFieldName);
        do {
          // array?
          if (lookup != null && lookup instanceof Integer) {
            BasicDBList temp = (BasicDBList) current;
            if (temp.get(lookup.toString()) == null) {
              if (pathParts.size() == 0
                  && !field.m_useIncomingFieldNameAsMongoFieldName) {
                // leaf - primitive element of the array
                boolean res = setMongoValueFromKettleValue(temp, lookup, vm,
                    row[index]);
                haveNonNullFields = (haveNonNullFields || res);
              } else {
                // must be a record here (since multi-dimensional array creation
                // is handled
                // in getPathElementName())

                // need to create this record/object
                BasicDBObject newRec = new BasicDBObject();
                temp.put(lookup.toString(), newRec);
                current = newRec;

                // end of the path?
                if (pathParts.size() == 0) {
                  if (field.m_useIncomingFieldNameAsMongoFieldName) {
                    boolean res = setMongoValueFromKettleValue(current,
                        incomingFieldName, vm, row[index]);
                    haveNonNullFields = (haveNonNullFields || res);
                  } else {
                    throw new KettleException(
                        BaseMessages
                            .getString(PKG,
                                "MongoDbOutput.Messages.Error.NoFieldNameSpecifiedForPath"));
                  }
                }
              }
            } else {
              // existing element of the array
              current = (DBObject) temp.get(lookup.toString());

              // no more path parts so we must be setting a field in an array
              // element
              // that is a record
              if (pathParts == null || pathParts.size() == 0) {
                if (current instanceof BasicDBObject) {
                  if (field.m_useIncomingFieldNameAsMongoFieldName) {
                    boolean res = setMongoValueFromKettleValue(current,
                        incomingFieldName, vm, row[index]);
                    haveNonNullFields = (haveNonNullFields || res);
                  } else {
                    throw new KettleException(
                        BaseMessages
                            .getString(PKG,
                                "MongoDbOutput.Messages.Error.NoFieldNameSpecifiedForPath"));
                  }
                }
              }
            }
          } else {
            // record/object
            if (lookup == null && pathParts.size() == 0) {
              if (field.m_useIncomingFieldNameAsMongoFieldName) {
                boolean res = setMongoValueFromKettleValue(current,
                    incomingFieldName, vm, row[index]);
                haveNonNullFields = (haveNonNullFields || res);
              } else {
                throw new KettleException(BaseMessages.getString(PKG,
                    "MongoDbOutput.Messages.Error.NoFieldNameSpecifiedForPath"));
              }
            } else {
              if (pathParts.size() == 0) {
                if (!field.m_useIncomingFieldNameAsMongoFieldName) {
                  boolean res = setMongoValueFromKettleValue(current,
                      lookup.toString(), vm, row[index]);
                  haveNonNullFields = (haveNonNullFields || res);
                } else {
                  current = (DBObject) current.get(lookup.toString());
                  boolean res = setMongoValueFromKettleValue(current,
                      incomingFieldName, vm, row[index]);
                  haveNonNullFields = (haveNonNullFields || res);
                }
              } else {
                current = (DBObject) current.get(lookup.toString());
              }
            }
          }

          lookup = getPathElementName(pathParts, current,
              field.m_useIncomingFieldNameAsMongoFieldName);
        } while (lookup != null);
      }
    }

    if (!haveNonNullFields) {
      return null; // nothing has been set!
    }

    return root;
  }

  private static boolean setMongoValueFromKettleValue(DBObject mongoObject,
      Object lookup, ValueMetaInterface kettleType, Object kettleValue)
      throws KettleValueException {
    if (kettleType.isNull(kettleValue)) {
      return false; // don't insert nulls!
    }

    if (kettleType.isString()) {
      String val = kettleType.getString(kettleValue);
      mongoObject.put(lookup.toString(), val);
      return true;
    }
    if (kettleType.isBoolean()) {
      Boolean val = kettleType.getBoolean(kettleValue);
      mongoObject.put(lookup.toString(), val);
      return true;
    }
    if (kettleType.isInteger()) {
      Long val = kettleType.getInteger(kettleValue);
      mongoObject.put(lookup.toString(), val.longValue());
      return true;
    }
    if (kettleType.isDate()) {
      Date val = kettleType.getDate(kettleValue);
      mongoObject.put(lookup.toString(), val);
      return true;
    }
    if (kettleType.isNumber()) {
      Double val = kettleType.getNumber(kettleValue);
      mongoObject.put(lookup.toString(), val.doubleValue());
      return true;
    }
    if (kettleType.isBigNumber()) {
      // use string value - user can use Kettle to convert back
      String val = kettleType.getString(kettleValue);
      mongoObject.put(lookup.toString(), val);
      return true;
    }
    if (kettleType.isBinary()) {
      byte[] val = kettleType.getBinary(kettleValue);
      mongoObject.put(lookup.toString(), val);
      return true;
    }
    if (kettleType.isSerializableType()) {
      throw new KettleValueException(BaseMessages.getString(PKG,
          "MongoDbOutput.Messages.Error.CantStoreKettleSerializableVals"));
    }

    return false;
  }

  private static Object getPathElementName(List<String> pathParts,
      DBObject current, boolean incomingAsFieldName) throws KettleException {

    if (pathParts == null || pathParts.size() == 0) {
      return null;
    }

    String part = pathParts.get(0);
    if (part.startsWith("[")) {
      String index = part.substring(1, part.indexOf(']')).trim();
      part = part.substring(part.indexOf(']') + 1).trim();
      if (part.length() > 0) {
        // any remaining characters must indicate a multi-dimensional array
        pathParts.set(0, part);

        // does this next array exist?
        if (current.get(index) == null) {
          BasicDBList newArr = new BasicDBList();
          current.put(index, newArr);
        }
      } else {
        // remove - we're finished with this part
        pathParts.remove(0);
      }
      return new Integer(index);
    } else if (part.endsWith("]")) {
      String fieldName = part.substring(0, part.indexOf('['));
      Object mongoField = current.get(fieldName);
      if (mongoField == null) {
        // create this field
        BasicDBList newField = new BasicDBList();
        current.put(fieldName, newField);
      } else {
        // check type - should be an array
        if (!(mongoField instanceof BasicDBList)) {
          throw new KettleException(BaseMessages.getString(PKG,
              "MongoDbOutput.Messages.Error.FieldExistsButIsntAnArray", part));
        }
      }
      part = part.substring(part.indexOf('['));
      pathParts.set(0, part);

      return fieldName;
    }

    // otherwise this path part is a record (object) or possibly a leaf (if we
    // are not
    // using the incoming kettle field name as the mongo field name)
    Object mongoField = current.get(part);
    if (mongoField == null) {
      if (incomingAsFieldName || pathParts.size() > 1) {
        // create this field
        BasicDBObject newField = new BasicDBObject();
        current.put(part, newField);
      }
    } else {
      // check type = should be a record (object)
      if (!(mongoField instanceof BasicDBObject) && pathParts.size() > 1) {
        throw new KettleException(BaseMessages.getString(PKG,
            "MongoDbOutput.Messages.Error.FieldExistsButIsntARecord", part));
      }
    }
    pathParts.remove(0);
    return part;
  }

  /**
   * Determines the top level structure of the outgoing Mongo document from the
   * user-specified field paths. This can be either RECORD ( for a top level
   * structure that is an object), ARRAY or INCONSISTENT (if the user has some
   * field paths that start with an array and some that start with an object).
   * 
   * @param fieldDefs the list of document field paths
   * @param vars environment variables
   * @return the top level structure
   */
  protected static MongoTopLevel checkTopLevelConsistency(
      List<MongoDbOutputMeta.MongoField> fieldDefs, VariableSpace vars) {

    int numRecords = 0;
    int numArrays = 0;

    for (MongoDbOutputMeta.MongoField field : fieldDefs) {
      String mongoPath = vars.environmentSubstitute(field.m_mongoDocPath);

      if (Const.isEmpty(mongoPath)) {
        numRecords++;
      } else if (mongoPath.startsWith("[")) {
        numArrays++;
      } else {
        numRecords++;
      }
    }

    if (numRecords < fieldDefs.size() && numArrays < fieldDefs.size()) {
      return MongoTopLevel.INCONSISTENT;
    }

    if (numRecords > 0) {
      return MongoTopLevel.RECORD;
    }

    return MongoTopLevel.ARRAY;
  }

  public static void main(String[] args) {
    try {
      List<MongoDbOutputMeta.MongoField> paths = new ArrayList<MongoDbOutputMeta.MongoField>();

      MongoDbOutputMeta.MongoField mf = new MongoDbOutputMeta.MongoField();

      mf.m_incomingFieldName = "field1";
      mf.m_mongoDocPath = "bob.fred[0].george";
      mf.m_useIncomingFieldNameAsMongoFieldName = true;
      mf.m_modifierUpdateOperation = "$set";
      mf.m_modifierOperationApplyPolicy = "Insert&Update";
      paths.add(mf);

      mf = new MongoDbOutputMeta.MongoField();
      mf.m_incomingFieldName = "field2";
      mf.m_mongoDocPath = "bob.fred[0].george";
      mf.m_useIncomingFieldNameAsMongoFieldName = true;
      mf.m_modifierUpdateOperation = "$set";
      mf.m_modifierOperationApplyPolicy = "Insert&Update";
      paths.add(mf);

      RowMetaInterface rmi = new RowMeta();
      ValueMetaInterface vm = new ValueMeta();
      vm.setName("field1");
      vm.setType(ValueMetaInterface.TYPE_STRING);
      rmi.addValueMeta(vm);
      vm = new ValueMeta();
      vm.setName("field2");
      vm.setType(ValueMetaInterface.TYPE_STRING);
      rmi.addValueMeta(vm);

      Object[] row = new Object[2];
      row[0] = "value1";
      row[1] = "value2";
      VariableSpace vs = new Variables();

      DBObject result = new MongoDbOutputData().getModifierUpdateObject(paths,
          rmi, row, vs, MongoDbOutputData.MongoTopLevel.RECORD);

      System.out.println(result);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }
}
