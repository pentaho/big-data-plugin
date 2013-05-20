package org.pentaho.hadoop.shim.common;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

/**
 * DriverProxyInvocationChain is a temporary solution for interacting with Hive drivers. At the time this class was added,
 * many methods from the JDBC API had not yet been implemented and would instead throw SQLExceptions. Also, some methods
 * such as HiveDatabaseMetaData.getTables() did not return any values.  For these reasons, a dynamic proxy chain was put in
 * place, in order to intercept methods that would otherwise not function properly, and instead inject working and/or 
 * default functionality.
 * 
 * The "chain" part of this class is a result of not having access to all the necessary objects at driver creation time.
 * For this reason, we have to intercept methods that would return such objects, then create and return a proxy to those
 * objects. There are a number of objects and methods to which this applies, so the result is a "chain" of getting
 * access to objects via a proxy, then returning a proxy to those objects, which in turn may return proxied objects for its
 * methods, and so on.
 * 
 * The large amount of reflection used here is because not all Hadoop distributions support both Hive and Hive 2. Thus 
 * before proxying or anything, we need to make sure we have the classes we need at runtime.
 */
public class DriverProxyInvocationChain {
  
  /** The initialized. */
  protected static boolean initialized = false;
  
  /** The hive1 db meta data class. */
  protected static Class<? extends DatabaseMetaData> hive1DbMetaDataClass = null;
  
  /** The hive2 db meta data class. */
  protected static Class<? extends DatabaseMetaData> hive2DbMetaDataClass = null;
  
  /** The hive1 result set class. */
  protected static Class<? extends ResultSet> hive1ResultSetClass = null;
  
  /** The hive2 result set class. */
  protected static Class<? extends ResultSet> hive2ResultSetClass = null;
  
  /** The hive1 client class. */
  protected static Class<?> hive1ClientClass = null;
  
  /** The hive2 client class. */
  protected static Class<?> hive2ClientClass = null;
  
  /** The hive1 statement class. */
  protected static Class<? extends Statement> hive1StatementClass = null;
  
  /** The hive2 statement class. */
  protected static Class<? extends Statement> hive2StatementClass = null;
  
  /**
   * Gets the proxy.
   *
   * @param intf the intf
   * @param obj the obj
   * @return the proxy
   */
  public static Driver getProxy(Class<? extends Driver> intf, final Driver obj) {
    if(!initialized) {
      init();
    }
      return (Driver) Proxy.newProxyInstance(obj.getClass().getClassLoader(),
                        new Class[] { intf }, new DriverInvocationHandler(obj));
  }
  
  /**
   * Inits the.
   */
  protected static void init() {
    
    // Get all the Hive 1 and Hive 2 classes we'll need to call methods on later. 
    
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      hive1DbMetaDataClass = (Class<? extends DatabaseMetaData>) Class.forName("org.apache.hadoop.hive.jdbc.HiveDatabaseMetaData", false, cl);
      hive1ResultSetClass = (Class<? extends ResultSet>) Class.forName("org.apache.hadoop.hive.jdbc.HiveQueryResultSet", false, cl);
      hive1ClientClass = Class.forName("org.apache.hadoop.hive.service.HiveInterface", false, cl);
      hive1StatementClass = (Class<? extends Statement>) Class.forName("org.apache.hadoop.hive.jdbc.HiveStatement", false, cl);
    } catch(ClassNotFoundException cnfe) {}
    
    try {
      hive2DbMetaDataClass = (Class<? extends DatabaseMetaData>) Class.forName("org.apache.hive.jdbc.HiveDatabaseMetaData", false, cl);
      hive2ResultSetClass = (Class<? extends ResultSet>) Class.forName("org.apache.hive.jdbc.HiveQueryResultSet", false, cl);
      hive2ClientClass = Class.forName("org.apache.hive.service.cli.thrift.TCLIService.Iface", false, cl);
      hive2StatementClass = (Class<? extends Statement>) Class.forName("org.apache.hive.jdbc.HiveStatement", false, cl);
    } catch(ClassNotFoundException cnfe) {}
    
    initialized = true;
  }

  /**
   * DriverInvocationHandler is a proxy handler class for java.sql.Driver. However the code in this file is
   * specifically for handling Hive JDBC calls, and therefore should not be used to proxy any other JDBC objects
   * besides those provided by Hive. 
   */
  private static class DriverInvocationHandler implements InvocationHandler {
    
    /** The driver. */
    Driver driver;
    
    /**
     * Instantiates a new Driver proxy handler.
     *
     * @param obj the Driver to proxy
     */
    public DriverInvocationHandler(Driver obj) {
        driver = obj;
    }

      /**
       * Intercepts methods called on the Driver to possibly perform alternate processing.
       *
       * @param proxy the proxy object
       * @param method the method being invoked
       * @param args the arguments to the method
       * @return the object returned by whatever processing takes place
       * @throws Throwable if an error occurs during processing
       */
      @Override
      public Object invoke(final Object proxy, Method method, Object[] args) throws Throwable {
          
        try {
          Object o = method.invoke(driver, args);
          if(o instanceof Connection) {
            
            // Intercept the Connection object so we can proxy that too
            return (Connection)Proxy.newProxyInstance(o.getClass().getClassLoader(),
                new Class[] { Connection.class }, new ConnectionInvocationHandler((Connection)o));
          }
          else {
            return o;
          }
        }
        catch(Throwable t) {
          throw (t instanceof InvocationTargetException) ? t.getCause() : t;
        }
      }
  }
  
  /**
   * ConnectionInvocationHandler is a proxy handler class for java.sql.Connection. However the code in this file is
   * specifically for handling Hive JDBC calls, and therefore should not be used to proxy any other JDBC objects
   * besides those provided by Hive. 
   */
  private static class ConnectionInvocationHandler implements InvocationHandler {

    /** The "real" connection. */
    Connection connection;
    
    /**
     * Instantiates a new connection invocation handler.
     *
     * @param obj the obj
     */
    public ConnectionInvocationHandler(Connection obj) {
      connection = obj;
    }

    /**
     * Intercepts methods called on the Connection to possibly perform alternate processing.
     *
     * @param proxy the proxy
     * @param method the method
     * @param args the args
     * @return the object
     * @throws Throwable the throwable
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      Object o = null;
      try {
        o = method.invoke(connection, args);
      }
      catch(Throwable t) {

        if(t instanceof InvocationTargetException) {
          Throwable cause = t.getCause();
        
          if(cause instanceof SQLException) {
            if(cause.getMessage().equals("Method not supported")) {
              String methodName = method.getName();
              if("createStatement".equals(methodName)) {
                o = createStatement(connection,args);
              }
              else if("setReadOnly".equals(methodName)) {
                o = (Void)null;
              }
              else {
                StringBuffer sb = new StringBuffer("Intercepted ");
                sb.append(method.getDeclaringClass().getName());
                sb.append(".");
                sb.append(methodName);
                sb.append(" with MethodNotSupported");
                System.err.println(sb);
                throw cause;
              }
            }
            else throw cause;
          }
          else {
            throw cause;
          }
        }
        else {
          throw t;
        }
      
      }
      if(o instanceof DatabaseMetaData) {
        DatabaseMetaData dbmd = (DatabaseMetaData)o;
        
        // Intercept the DatabaseMetaData object so we can proxy that too
        return (DatabaseMetaData)Proxy.newProxyInstance(dbmd.getClass().getClassLoader(),
            new Class[] { DatabaseMetaData.class }, new DatabaseMetaDataInvocationHandler(dbmd));
      }
      else if(o instanceof PreparedStatement) {
        PreparedStatement st = (PreparedStatement)o;
        
        // Intercept the Statement object so we can proxy that too
        return (PreparedStatement)Proxy.newProxyInstance(st.getClass().getClassLoader(),
            new Class[] { PreparedStatement.class }, new CaptureResultSetInvocationHandler<PreparedStatement>(st));
      }
      else if(o instanceof Statement) {
        Statement st = (Statement)o;
        
        // Intercept the Statement object so we can proxy that too
        return (Statement)Proxy.newProxyInstance(st.getClass().getClassLoader(),
            new Class[] { Statement.class }, new CaptureResultSetInvocationHandler<Statement>(st));
      }
      else {
        return o;
      }      
    }
    
    /**
     * Creates a statement for the given Connection with the specified arguments
     *
     * @param c the connection object
     * @param args the arguments
     * @return the statement
     * @throws SQLException the sQL exception
     * @see java.sql.Connection#createStatement(int, int)
     */
    public Statement createStatement(Connection c, Object[] args) throws SQLException {
      if (c.isClosed()) {
        throw new SQLException("Can't create Statement, connection is closed ");
      }
      
      /* Ignore these for now -- this proxy stuff should go away anyway when the fixes are made to Apache Hive
       
      int resultSetType = (Integer)args[0];
      int resultSetConcurrency = (Integer)args[1];
      
      if(resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
        throw new SQLException(
            "Invalid parameter to createStatement() only TYPE_FORWARD_ONLY is supported ("+resultSetType+"!="+ResultSet.TYPE_FORWARD_ONLY+")");
      }
      
      if(resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
        throw new SQLException(
            "Invalid parameter to createStatement() only CONCUR_READ_ONLY is supported");
      }*/
      return c.createStatement();
    }
  }
  
 /**
  * DatabaseMetaDataInvocationHandler is a proxy handler class for java.sql.DatabaseMetaData. However the code in this file is
   * specifically for handling Hive JDBC calls, and therefore should not be used to proxy any other JDBC objects
   * besides those provided by Hive. 
  */
 private static class DatabaseMetaDataInvocationHandler implements InvocationHandler {
    
   /** The "real" database metadata object. */
   DatabaseMetaData t;
    
    /**
     * Instantiates a new database meta data invocation handler.
     *
     * @param t the database metadata object to proxy
     */
    public DatabaseMetaDataInvocationHandler(DatabaseMetaData t) {
      this.t = t;
    }

    /**
     * Intercepts methods called on the DatabaseMetaData object to possibly perform alternate processing.
     *
     * @param proxy the proxy
     * @param method the method
     * @param args the args
     * @return the object
     * @throws Throwable the throwable
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      
      try {
        
        // For Hive/Impala drivers, we need to intercept the getTables method even though it doesn't
        // throw an exception, because the ResultSet is empty. The temp fix is to try an execute a 
        // HiveQL query of "show tables". This only returns one (differently-named) column containing
        // the table/view name, vs. getTables() which returns much metadata. C'est la vie.
        
        if(hive1DbMetaDataClass != null && hive1DbMetaDataClass.isAssignableFrom(t.getClass())) {
          return getTables(t,hive1DbMetaDataClass,hive1StatementClass,hive1ClientClass,
              (String)args[0],(String)args[1],(String)args[2],(String[])args[3]);
        }
        if(hive2DbMetaDataClass != null && hive2DbMetaDataClass.isAssignableFrom(t.getClass())) {
          return getTables(t,hive2DbMetaDataClass,hive2StatementClass,hive2ClientClass,
              (String)args[0],(String)args[1],(String)args[2],(String[])args[3]);
        }
      
        // try to invoke the method as-is
        Object o = method.invoke(t, args);
        if(o instanceof ResultSet) {
          ResultSet r = (ResultSet)o;
          
          return (ResultSet)Proxy.newProxyInstance(r.getClass().getClassLoader(),
              new Class[] { ResultSet.class }, new ResultSetInvocationHandler(r));
        }
        else {
          return o;
        }
      }
      catch(Throwable t) {
        if(t instanceof InvocationTargetException) {
          Throwable cause = t.getCause();
        
          if(cause instanceof SQLException) {
            if(cause.getMessage().equals("Method not supported")) {
              // Intercept unsupported methods and inject some default code
              String methodName = method.getName();
              if("getIdentifierQuoteString".equals(methodName)) {
                return getIdentifierQuoteString();
              }
              else {
                StringBuffer sb = new StringBuffer("Intercepted ");
                sb.append(method.getDeclaringClass().getName());
                sb.append(".");
                sb.append(methodName);
                sb.append(" with MethodNotSupported");
                System.err.println(sb);
                throw cause;
              }
            }
            else throw cause;
          }
          else {
            throw cause;
          }
        }
        else {
          throw t;
        }
      }
    }
    
    /**
     * Returns the identifier quote string.  This is HiveQL specific
     *
     * @return String the quote string for identifiers in HiveQL
     * @throws SQLException if any SQL error occurs
     */
    public String getIdentifierQuoteString() throws SQLException {
      return "'";
    }
    
    /**
     * Gets the tables for the specified database.
     *
     * @param originalObject the original object
     * @param dbMetadataClass the db metadata class
     * @param statementClass the statement class
     * @param clientClass the client class
     * @param catalog the catalog
     * @param schemaPattern the schema pattern
     * @param tableNamePattern the table name pattern
     * @param types the types
     * @return the tables
     * @throws Exception the exception
     */
    public ResultSet getTables(Object originalObject, Class<? extends DatabaseMetaData> dbMetadataClass,
                               Class<? extends Statement> statementClass, Class<?> clientClass,
                               String catalog, String schemaPattern,
                               String tableNamePattern, String[] types) throws Exception {

      boolean tables = false;
      if(types == null) {
        tables = true;
      }
      else {
        for(String type : types) {
          if("TABLE".equals(type)) tables = true;
        }
      }
      
      // If we're looking for tables, execute "show tables" query instead
      if(tables) {
        Method getClient = dbMetadataClass.getDeclaredMethod("getClient");
        Constructor<? extends Statement> hiveStatementCtor = (Constructor<? extends Statement>) statementClass.getDeclaredConstructor(clientClass);
        Statement showTables = hiveStatementCtor.newInstance(clientClass.cast(getClient.invoke(originalObject)));
        showTables.executeQuery("show tables");
        ResultSet rs = showTables.getResultSet();
        return rs;
      }
      else {
        Method getTables = dbMetadataClass.getDeclaredMethod("getTables");
        ResultSet rs = (ResultSet)getTables.invoke(originalObject, catalog, schemaPattern, tableNamePattern, types);
        return rs;
      }
    }
  }
  
  /**
   * CaptureResultSetInvocationHandler is a generic proxy handler class for any java.sql.* class that has methods
   * to return ResultSet objects. However the code in this file is specifically for handling Hive JDBC calls, and 
   * therefore should not be used to proxy any other JDBC objects besides those provided by Hive. 
   *
   * @param <T> the generic type of object whose methods return ResultSet objects
   */
  private static class CaptureResultSetInvocationHandler<T> implements InvocationHandler {
    
    /** The object whose methods return ResultSet objects. */
    T t;
    
    /**
     * Instantiates a new capture result set invocation handler.
     *
     * @param t the t
     */
    public CaptureResultSetInvocationHandler(T t) {
      this.t = t;
    }

    /**
     * Intercepts methods called on the object to possibly perform alternate processing.
     *
     * @param proxy the proxy
     * @param method the method
     * @param args the args
     * @return the object
     * @throws Throwable the throwable
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      // try to invoke the method as-is
      try {
        Object o = method.invoke(t, args);
        if(o instanceof ResultSet) {
          ResultSet r = (ResultSet)o;
          
          return (ResultSet)Proxy.newProxyInstance(r.getClass().getClassLoader(),
              new Class[] { ResultSet.class }, new ResultSetInvocationHandler(r));
        }
        else if(o instanceof ResultSetMetaData) {
          ResultSetMetaData r = (ResultSetMetaData)o;
          
          return (ResultSetMetaData)Proxy.newProxyInstance(r.getClass().getClassLoader(),
              new Class[] { ResultSetMetaData.class }, new ResultSetMetaDataInvocationHandler(r));
        }
        else {
          return o;
        }
      }
      catch(Throwable t) {
        throw (t instanceof InvocationTargetException) ? t.getCause() : t;
      }
    }
  }

  /**
   * ResultSetInvocationHandler is a proxy handler class for java.sql.ResultSet. However the code in this file is
   * specifically for handling Hive JDBC calls, and therefore should not be used to proxy any other JDBC objects
   * besides those provided by Hive. 
   */
  private static class ResultSetInvocationHandler implements InvocationHandler {

    /** The "real" ResultSet object . */
    ResultSet rs;
    
    /**
     * Instantiates a new result set invocation handler.
     *
     * @param r the r
     */
    public ResultSetInvocationHandler(ResultSet r) {
      rs = r;
    }

    /**
     * Intercepts methods called on the ResultSet to possibly perform alternate processing.
     *
     * @param proxy the proxy
     * @param method the method
     * @param args the args
     * @return the object
     * @throws Throwable the throwable
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      
      try {
        // Intercept the getString(String) method to implement the hack for "show tables" vs. getTables()
        if("getString".equals(method.getName()) && args != null && args.length==1 && args[0] instanceof String) {
          return getString((String)args[0]);
        }
        else {
          Object o = method.invoke(rs,args);
          
          if(o instanceof ResultSetMetaData ) {
            // Intercept the ResultSetMetaData object so we can proxy that too
            return (ResultSetMetaData)Proxy.newProxyInstance(o.getClass().getClassLoader(),
                new Class[] { ResultSetMetaData.class }, new ResultSetMetaDataInvocationHandler((ResultSetMetaData)o));
          }
          else {
            return o;
          }
        }
      }
      catch(Throwable t) {
        throw (t instanceof InvocationTargetException) ? t.getCause() : t;
      }
    }
    
    /**
     * Gets the string value from the current row at the column with the specified name.
     *
     * @param columnName the column name
     * @return the string value of the row at the column with the specified name
     * @throws SQLException if the column name cannot be found
     */
    public String getString(String columnName) throws SQLException {
      
      String columnVal = null;
      SQLException exception = null;
      try {
        columnVal = rs.getString(columnName);
      }
      catch(SQLException se) {
        // Save for returning later
        exception = se;
      }
      if(columnVal != null) return columnVal;
      if(columnName != null && "TABLE_NAME".equals(columnName)) {
        if(columnName != null && "TABLE_NAME".equals(columnName)) {
          try {
            // If we're using the "show tables" hack in getTables(), return the first column
            columnVal = rs.getString(1);
          }
          catch(SQLException se) {
            throw (exception == null) ? se : exception;
          }
        }
      }
      return columnVal;
    }
  }
  
  /**
   * ResultSetMetaDataInvocationHandler is a proxy handler class for java.sql.ResultSetMetaData. However the code in 
   * this file is specifically for handling Hive JDBC calls, and therefore should not be used to proxy any other 
   * JDBC objects besides those provided by Hive. 
   */
  private static class ResultSetMetaDataInvocationHandler implements InvocationHandler {

    /** The "real" ResultSetMetaData object. */
    ResultSetMetaData rsmd;
    
    /**
     * Instantiates a new result set meta data invocation handler.
     *
     * @param r the r
     */
    public ResultSetMetaDataInvocationHandler(ResultSetMetaData r) {
      rsmd = r;
    }

    /**
     * Intercepts methods called on the ResultSetMetaData object to possibly perform alternate processing.
     *
     * @param proxy the proxy
     * @param method the method
     * @param args the args
     * @return the object
     * @throws Throwable the throwable
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      
      try {
        return method.invoke(rsmd, args);
      }
      catch(Throwable t) {
        if(t instanceof InvocationTargetException) {
          Throwable cause = t.getCause();
        
          if(cause instanceof SQLException) {
            if(cause.getMessage().equals("Method not supported")) {
              String methodName = method.getName();
              if("isSigned".equals(methodName)) {
                return isSigned((Integer)args[0]);
              }
              else {
                StringBuffer sb = new StringBuffer("Intercepted ");
                sb.append(method.getDeclaringClass().getName());
                sb.append(".");
                sb.append(methodName);
                sb.append(" with MethodNotSupported");
                System.err.println(sb);
                throw cause;
              }
            }
            else throw cause;
          }
          else {
            throw cause;
          }
        }
        else {
          throw t;
        }
      }
    }
    
    /**
     * Returns a true if values in the column are signed, false if not.
     * 
     * This method checks the type of the passed column.  If that
     * type is not numerical, then the result is false.
     * If the type is a numeric then a true is returned.
     *
     * @param column the index of the column to test
     * @return boolean
     * @throws SQLException the sQL exception
     */
    public boolean isSigned(int column) throws SQLException {
      int numCols = rsmd.getColumnCount();
      
      if (column < 1 || column > numCols) {
        throw new SQLException("Invalid column value: " + column);
      }

      // we need to convert the thrift type to the SQL type
      int type = rsmd.getColumnType(column);
      switch(type){
      case Types.DOUBLE: case Types.DECIMAL: case Types.FLOAT:
      case Types.INTEGER: case Types.REAL: case Types.SMALLINT: case Types.TINYINT:
      case Types.BIGINT:
        return true;
      }
      return false;
    }
  }
}

