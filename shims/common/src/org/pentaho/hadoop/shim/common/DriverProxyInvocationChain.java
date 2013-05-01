package org.pentaho.hadoop.shim.common;

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

public class DriverProxyInvocationChain {

  public static Driver getProxy(Class<? extends Driver> intf, 
    final Driver obj) {
      return (Driver) 
        Proxy.newProxyInstance(obj.getClass().getClassLoader(),
              new Class[] { intf }, new DriverInvocationHandler(obj));
  }
  
  private static class DriverInvocationHandler implements InvocationHandler {
    Driver driver;
    
    public DriverInvocationHandler(Driver obj) {
        driver = obj;
    }

      @Override
      public Object invoke(final Object proxy, Method method, 
        Object[] args) throws Throwable {
          
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
  
  private static class ConnectionInvocationHandler implements InvocationHandler {

    Connection connection;
    
    public ConnectionInvocationHandler(Connection obj) {
      connection = obj;
    }

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
    
    /*
     * (non-Javadoc)
     *
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
  
 private static class DatabaseMetaDataInvocationHandler implements InvocationHandler {
    
   DatabaseMetaData t;
    
    public DatabaseMetaDataInvocationHandler(DatabaseMetaData t) {
      this.t = t;
    }

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
        else {
          return o;
        }
      }
      catch(Throwable t) {
        if(t instanceof InvocationTargetException) {
          Throwable cause = t.getCause();
        
          if(cause instanceof SQLException) {
            if(cause.getMessage().equals("Method not supported")) {
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
     * Returns the identifier quote string.
     *
     * @return String
     * @throws
     */
    public String getIdentifierQuoteString() throws SQLException {
      return "'";
    }
  }
  
  private static class CaptureResultSetInvocationHandler<T> implements InvocationHandler {
    
    T t;
    
    public CaptureResultSetInvocationHandler(T t) {
      this.t = t;
    }

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

  private static class ResultSetInvocationHandler implements InvocationHandler {

    ResultSet rs;
    
    public ResultSetInvocationHandler(ResultSet r) {
      rs = r;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      
      try {
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
      catch(Throwable t) {
        throw (t instanceof InvocationTargetException) ? t.getCause() : t;
      }
    }
  }
  
  private static class ResultSetMetaDataInvocationHandler implements InvocationHandler {

    ResultSetMetaData rsmd;
    
    public ResultSetMetaDataInvocationHandler(ResultSetMetaData r) {
      rsmd = r;
    }

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
     * Returns a true if column is signed, false if not.
     *
     * This method checks the type of the passed column.  If that
     * type is not numerical, then the result is false.
     * If the type is a numeric then a true is returned.
     *
     * @param column the index of the column to test
     * @return boolean
     * @throws
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

