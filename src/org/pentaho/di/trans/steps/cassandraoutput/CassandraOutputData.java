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

package org.pentaho.di.trans.steps.cassandraoutput;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.Deflater;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.pentaho.cassandra.CassandraColumnMetaData;
import org.pentaho.cassandra.CassandraConnection;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

/**
 * Data class for the CassandraOutput step. Contains methods for obtaining
 * a connection to cassandra, creating a new column family, updating a column
 * family's meta data and constructing a batch insert CQL statement.
 * 
 * @author Mark Hall (mhall{[at]}pentaho{[dot]}com)
 * @version $Revision$
 */
public class CassandraOutputData extends BaseStepData implements
    StepDataInterface {
  
  /** The output data format */
  protected RowMetaInterface m_outputRowMeta;
    
  /**
   * Get the output row format
   * 
   * @return the output row format
   */
  public RowMetaInterface getOutputRowMeta() {
    return m_outputRowMeta;
  }
  
  /**
   * Set the output row format
   * 
   * @param rmi the output row format
   */
  public void setOutputRowMeta(RowMetaInterface rmi) {
    m_outputRowMeta = rmi;
  }
  
  /**
   * Get a connection to cassandra
   * 
   * @param host the hostname of a cassandra node
   * @param port the port that cassandra is listening on
   * @param username the username for (optional) authentication
   * @param password the password for (optional) authentication
   * @return a connection to cassandra
   * @throws Exception if a problem occurs during connection
   */
  public static CassandraConnection getCassandraConnection(String host, 
      int port, String username, String password) throws Exception {
    return new CassandraConnection(host, port, username, password, -1);
  }
  
  /**
   * Begin a new batch cql statement
   * 
   * @param numRows the number of rows to be inserted in this batch
   * @param consistency the consistency (e.g. ONE, QUORUM etc.) to use, or null
   * to use the default.
   * 
   * @return a StringBuilder initialized for the batch.
   */
  public static StringBuilder newBatch(int numRows, String consistency) {
    
    // make a stab at a reasonable initial capacity
    StringBuilder batch = new StringBuilder(numRows * 80);
    batch.append("BEGIN BATCH");
    
    if (!Const.isEmpty(consistency)) {
      batch.append(" USING CONSISTENCY ").append(consistency);
    }
    
    batch.append("\n");
    
    return batch;
  }
  
  /**
   * Append the "APPLY BATCH" statement to complete the batch
   * 
   * @param batch the StringBuilder batch to complete
   */
  public static void completeBatch(StringBuilder batch) {
    batch.append("APPLY BATCH");
  }
  
  /**
   * Send the batch insert.
   * 
   * @param batch the CQL batch insert statement
   * @param conn the connection to use
   * @param compressCQL true if the CQL should be compressed
   * @throws Exception if a problem occurs
   */
  public static void commitBatch(StringBuilder batch, CassandraConnection conn,
      boolean compressCQL) throws Exception {
    
    //System.out.println(batch.toString());
    // compress the batch if necessary
    byte[] toSend = null;
    if (compressCQL) {
      toSend = compressQuery(batch.toString(), Compression.GZIP);
    } else {
      toSend = batch.toString().getBytes(Charset.forName(CassandraColumnMetaData.UTF8));
    }
    
    conn.getClient().execute_cql_query(ByteBuffer.wrap(toSend), 
        compressCQL ? Compression.GZIP : Compression.NONE);
  }
  
  /**
   * converts a kettle row to CQL insert statement and adds it to 
   * the batch
   * 
   * @param batch StringBuilder for collecting the batch CQL
   * @param colFamilyName the name of the column family (table) to
   * insert into
   * @param inputMeta Kettle input row meta data
   * @param keyIndex the index of the incoming field to use as the
   * key for inserting
   * @param row the Kettle row
   * @param cassandraMeta meta data on the columns in the cassandra
   * column family (table)
   * @param insertFieldsNotInMetaData true if any Kettle fields that
   * are not in the Cassandra column family (table) meta data are to be
   * inserted. This is irrelevant if the user has opted
   * to have the step initially update the Cassandra meta data for incoming
   * fields that are not known about.
   * 
   * @return true if the row was added to the batch
   * 
   * @throws KettleException if the key is null in the incoming row
   */
  public static boolean addRowToBatch(StringBuilder batch, String colFamilyName, 
      RowMetaInterface inputMeta, int keyIndex, Object[] row, 
      CassandraColumnMetaData cassandraMeta, boolean insertFieldsNotInMetaData) 
    throws KettleException {
    
    // check the key first
    ValueMetaInterface keyMeta = inputMeta.getValueMeta(keyIndex);
    if (keyMeta.isNull(row[keyIndex])) {
      //throw new KettleException("Can't insert this row because the key is null!");
      System.err.println("Skipping this row because the key is null! " + row);
      return false;
    }
    
    // quick scan to see if we have at least one non-null value apart from
    // the key
    boolean ok = false;
    for (int i = 0; i < inputMeta.size(); i++) {
      if (i != keyIndex) {
        ValueMetaInterface v = inputMeta.getValueMeta(i);
        if (!v.isNull(row[i])) {
          ok = true;
          break;
        }
      }
    }
    if (!ok) {
      System.err.println("Skipping row with key '" + keyMeta.getString(row[keyIndex])
          +"' because there are no non-null values!");
      return false;
    }
    
    batch.append("INSERT INTO ").append(colFamilyName).append(" (KEY");
    
    for (int i = 0; i < inputMeta.size(); i++) {
      if (i != keyIndex) {
        ValueMetaInterface colMeta = inputMeta.getValueMeta(i);
        String colName = colMeta.getName();
        if (!cassandraMeta.columnExistsInSchema(colName) && 
            !insertFieldsNotInMetaData) {
          continue;
        }
        
        // don't insert if null!
        if (colMeta.isNull(row[i])) {
          continue;
        }
        
        batch.append(", '").append(colName).append("'");
      }
    }
    
    batch.append(") VALUES (");
    // key first
    String keyString = CassandraColumnMetaData.kettleValueToCQL(keyMeta, row[keyIndex]);
    //String keyString = keyMeta.getString(row[keyIndex]);
    batch.append("'").append(keyString).append("'");
    
    for (int i = 0; i < inputMeta.size(); i++) {
      if (i != keyIndex) {
        ValueMetaInterface colMeta = inputMeta.getValueMeta(i);
        String colName = colMeta.getName();
        if (!cassandraMeta.columnExistsInSchema(colName) && 
            !insertFieldsNotInMetaData) {
          continue;
        }
        
        // don't insert if null!
        if (colMeta.isNull(row[i])) {
          continue;
        }
        
        //batch.append(", '").append(colMeta.getString(row[i])).append("'");
        batch.append(", '").append(CassandraColumnMetaData.kettleValueToCQL(colMeta, row[i])).append("'");
      }
    }
    
    batch.append(")\n");
    
    return true;
  }
  
  protected static int numFieldsToBeWritten(String colFamilyName, RowMetaInterface inputMeta,
      int keyIndex, CassandraColumnMetaData cassandraMeta, boolean insertFieldsNotInMetaData) {
    
    // check how many fields will actually be inserted - we must insert at least one field
    // appart from the key or Cassandra will complain.
    
    // TODO
    
    int count = 1; // key
    for (int i = 0; i < inputMeta.size(); i++) {
      if (i != keyIndex) {
        ValueMetaInterface colMeta = inputMeta.getValueMeta(i);
        String colName = colMeta.getName();
        if (!cassandraMeta.columnExistsInSchema(colName) && 
            !insertFieldsNotInMetaData) {
          continue;
        }
        count++;
      }
    }
    
    return count;
  }
  
  /**
   * Constructs and executes a CQL TRUNCATE statement.
   * 
   * @param conn the connection to use
   * @param columnFamily the name of the column family to truncate.
   * @throws Exception if a problem occurs.
   */
  public static void truncateColumnFamily(CassandraConnection conn,
      String columnFamily) throws Exception {
    String cqlCommand = "TRUNCATE " + columnFamily;
    
    conn.getClient().execute_cql_query(ByteBuffer.wrap(cqlCommand.getBytes()), 
        Compression.NONE);
  }
  
  /**
   * Updates the schema information for a given column family with any
   * fields in the supplied RowMeta that aren't defined in the
   * schema. Abuses the schema "comment" field to store information on
   * any indexed values that might be in the fields in the RowMeta.
   * 
   * @param conn the connection to use
   * @param colFamilyName the name of the column family to update
   * @param inputMeta the row meta containing (potentially) new fields
   * @param keyIndex the index of the key field in the row meta
   * @param cassandraMeta meta data for the cassandra column family
   * @throws Exception if a problem occurs updating the schema
   */
  public static void updateCassandraMeta(CassandraConnection conn, 
      String colFamilyName, RowMetaInterface inputMeta,
      int keyIndex, CassandraColumnMetaData cassandraMeta) throws Exception {
    
    // column families               
    KsDef keySpace = conn.describeKeyspace();
    List<CfDef> colFams = null;
    if (keySpace != null) {
      colFams = keySpace.getCf_defs();
    } else {
      throw new Exception("Unable to get meta data on keyspace.");
    }

    // look for the requested column family
    CfDef colFamDefToUpdate = null;
//    CfDef colDefs = null;
    for (CfDef fam : colFams) {
      String columnFamilyName = fam.getName(); // table name
      if (columnFamilyName.equals(colFamilyName)) {
        colFamDefToUpdate = fam;
        break;
      }
    }
    
    if (colFamDefToUpdate == null) {
      throw new Exception("Can't update meta data - unable to find " +
      		"column family '" + colFamilyName + "'");
    }
    
    String comment = colFamDefToUpdate.getComment();

    List<ValueMetaInterface> indexedVals = new ArrayList<ValueMetaInterface>();        
    for (int i = 0; i < inputMeta.size(); i++) {
      if (i != keyIndex) {
        ValueMetaInterface colMeta = inputMeta.getValueMeta(i);
        if (colMeta.getStorageType() == ValueMetaInterface.STORAGE_TYPE_INDEXED) {
          indexedVals.add(colMeta);
        }
        String colName = colMeta.getName();
        if (!cassandraMeta.columnExistsInSchema(colName)) {
          String colType = CassandraColumnMetaData.getCassandraTypeForValueMeta(colMeta);
          
          ColumnDef newCol = new ColumnDef(ByteBuffer.wrap(colName.getBytes()),                                              
            colType);
          colFamDefToUpdate.addToColumn_metadata(newCol);
        }
      }     
    }    
    
    // update the comment fields for any new indexed vals
    if (indexedVals.size() > 0) {
      String before = "";
      String after = "";
      String meta = "";
      if (comment != null && comment.length() > 0) {
        // is there any indexed value meta data there already?
        if (comment.indexOf("@@@") >= 0) {
          // have to strip out existing stuff
          before = comment.substring(0, comment.indexOf("@@@"));
          after = comment.substring(comment.lastIndexOf("@@@") + 3, comment.length());
          meta = comment.substring(comment.indexOf("@@@", comment.lastIndexOf("@@@")));
          meta = meta.replace("@@@", "");
        }
      }
      
      StringBuffer buff = new StringBuffer();
      buff.append(meta);
      for (ValueMetaInterface vm : indexedVals) {
        String colName = vm.getName();
        if (meta.indexOf(colName) < 0) {
          // add this one
          Object[] legalVals = vm.getIndex();
          if (buff.length() > 0) {
            buff.append(";").append(colName).append(":{");
          } else {
            buff.append(colName).append(":{");
          }
          for (int i = 0; i < legalVals.length; i++) {
            buff.append(legalVals[i].toString());
            if (i != legalVals.length - 1) {
              buff.append(",");
            }
          }
          buff.append("}");
        }
      }
      
      comment = before + "@@@" + buff.toString() + "@@@" + after;
      colFamDefToUpdate.setComment(comment);
    }
    
    conn.getClient().system_update_column_family(colFamDefToUpdate);
    
    // get the cassandraMeta to refresh itself
    cassandraMeta.refresh(conn);
  }
  
  /**
   * Static utility method that executes a set of semicolon separated 
   * CQL commands against a keyspace. In the context of CassandraOutput
   * this method can be used to execute CQL commands (to create secondary
   * indexes for example) before rows are inserted into the column family
   * in question.
   * 
   * @param conn the connection to use
   * @param cql the string containing the semicolon separated cql commands
   * to execute
   * @param log the logging object to log errors to
   * @param compressCQL true if the cql commands should be compressed before
   * sending to the server.
   */
  public static void executeAprioriCQL(CassandraConnection conn, String cql, 
      LogChannelInterface log, boolean compressCQL) {
    
    // split out separate statements
    String[] cqlRequests = cql.split(";");
    if (cqlRequests.length > 0) {
      for (String cqlC : cqlRequests) {
        cqlC = cqlC.trim();
        if (!cqlC.endsWith(";")) {
          cqlC += ";";
        }
        
        // try and execute it
        byte[] toSend = null;
        if (compressCQL) {
          toSend = compressQuery(cqlC, Compression.GZIP);
        } else {
          toSend = cqlC.getBytes(Charset.forName(CassandraColumnMetaData.UTF8));
        }

        String errorMessage = null;
        try {
          conn.getClient().execute_cql_query(ByteBuffer.wrap(toSend), 
              compressCQL ? Compression.GZIP : Compression.NONE);
        } catch (InvalidRequestException e) {
          errorMessage = e.why;
        } catch (UnavailableException e) {
          errorMessage = e.getMessage();
        } catch (TimedOutException e) {
          errorMessage = e.getMessage();
        } catch (SchemaDisagreementException e) {            
          errorMessage = e.getMessage();
        } catch (TException e) {
          errorMessage = e.getMessage();
        }

        if (errorMessage != null) {
          log.logBasic("Unable to execute a priori CQL command '" + cqlC +
              "'. (" + errorMessage + ")");
        }       
      }
    }
  }
  
  /**
   * Constructs a CQL statement to create a new column family. Uses
   * Cassandra defaults for default comparator, key_cache size etc. at present.
   * 
   * @param conn the connection to use
   * @param colFamilyName the name of the column family (table) to create.
   * @param inputMeta the row meta information on the incoming fields to be inserted
   * into this new column family
   * @param keyIndex the index of the incoming field that is to be used as the key
   * for the new column family
   * @param compressCQL true if the CQL statement is to be compressed before sending
   * to the server
   * @throws Exception if a problem occurs.
   */
  public static boolean createColumnFamily(CassandraConnection conn, 
      String colFamilyName, RowMetaInterface inputMeta,
      int keyIndex, boolean compressCQL) throws Exception {
    
    // TODO handle optional keywords for column family creation - default comparator, key_cache_size etc.
    // Will require more UI and Meta class setters/getters
    
    StringBuffer buff = new StringBuffer();
    buff.append("CREATE COLUMNFAMILY " + colFamilyName);
    ValueMetaInterface kvm = inputMeta.getValueMeta(keyIndex);
    buff.append(" (KEY ").append("'" 
        + CassandraColumnMetaData.getCQLTypeForValueMeta(kvm) + "'");
    buff.append(" PRIMARY KEY");
    
    List<ValueMetaInterface> indexedVals = new ArrayList<ValueMetaInterface>();
    if (inputMeta.size() > 1) {
      
      //boolean first = true;
      for (int i = 0; i < inputMeta.size(); i++) {
        if (i != keyIndex) {
          ValueMetaInterface vm = inputMeta.getValueMeta(i);
          if (vm.getStorageType() == ValueMetaInterface.STORAGE_TYPE_INDEXED) {
            indexedVals.add(vm);
          }
          String colName = vm.getName();
          String colType = "'" + CassandraColumnMetaData.getCQLTypeForValueMeta(vm) + "'";
          //  if (!first) {
          buff.append(", ");
          //  first = false;
          /*          } else {
            buff.append(colName).append(" ");
          } */
          buff.append("'" + colName + "'").append(" ");
          buff.append(colType);
        }
      }
    } else {
      return false; // we can't insert any data if there is only the key coming into the step     
    }
    
    // abuse the comment field to store any indexed values :-)
    if (indexedVals.size() == 0) {
      buff.append(");");
    } else {
      buff.append(") WITH comment = '@@@");
      int count = 0;
      for (ValueMetaInterface vm : indexedVals) {
        String colName = vm.getName();
        Object[] legalVals = vm.getIndex();
        buff.append(colName).append(":{");
        for (int i = 0; i < legalVals.length; i++) {
          buff.append(legalVals[i].toString());
          if (i != legalVals.length - 1) {
            buff.append(",");
          }
        }
        buff.append("}");
        if (count != indexedVals.size() - 1) {
          buff.append(";");
        }
        count++;
      }
      buff.append("@@@';");
    }    
    System.out.println(buff.toString());
        
    byte[] toSend = null;
    if (compressCQL) {
      toSend = compressQuery(buff.toString(), Compression.GZIP);
    } else {
      toSend = buff.toString().getBytes(Charset.forName(CassandraColumnMetaData.UTF8));
    }
    conn.getClient().execute_cql_query(ByteBuffer.wrap(toSend), 
        compressCQL ? Compression.GZIP : Compression.NONE);    
    
    return true;
  }
  
  /**
   * Compress a CQL query
   * 
   * @param queryStr the CQL query
   * @param compression compression option (GZIP is the only option - so far)
   * @return an array of bytes containing the compressed query
   */
  public static byte[] compressQuery(String queryStr, Compression compression) {
    byte[] data = queryStr.getBytes(Charset.forName(CassandraColumnMetaData.UTF8));
    
    Deflater compressor = new Deflater();
    compressor.setInput(data);
    compressor.finish();

    ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
    byte[] buffer = new byte[1024];

    while (!compressor.finished()) {
      int size = compressor.deflate(buffer);
      byteArray.write(buffer, 0, size);
    }
    
    return byteArray.toByteArray();
  }

}
