/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package org.pentaho.hadoop;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.InvalidPropertiesFormatException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * Created by bryan on 8/6/15.
 */
public class PropertiesConfigurationProperties extends Properties {
  private final PropertiesConfiguration propertiesConfiguration;

  public PropertiesConfigurationProperties( FileObject fileObject ) throws ConfigurationException, FileSystemException {
    this( initPropertiesConfiguration( fileObject ) );
  }

  public PropertiesConfigurationProperties( PropertiesConfiguration propertiesConfiguration ) {
    this.propertiesConfiguration = propertiesConfiguration;
  }

  private static PropertiesConfiguration initPropertiesConfiguration( FileObject fileObject )
    throws FileSystemException, ConfigurationException {
    PropertiesConfiguration propertiesConfiguration = new PropertiesConfiguration( fileObject.getURL() );
    propertiesConfiguration.setAutoSave( true );
    FileChangedReloadingStrategy fileChangedReloadingStrategy = new FileChangedReloadingStrategy();
    fileChangedReloadingStrategy.setRefreshDelay( 1000L );
    propertiesConfiguration.setReloadingStrategy( fileChangedReloadingStrategy );
    return propertiesConfiguration;
  }

  @Override public synchronized String getProperty( String key ) {
    return getProperty( key, null );
  }

  @Override public synchronized String getProperty( String key, String defaultValue ) {
    return propertiesConfiguration.getString( key, defaultValue );
  }

  @Override public synchronized Object get( Object key ) {
    if ( key == null || key instanceof String ) {
      return propertiesConfiguration.getProperty( (String) key );
    } else {
      return null;
    }
  }

  @Override public synchronized Object setProperty( String key, String value ) {
    return put( key, value );
  }

  @Override public synchronized Object put( Object key, Object value ) {
    if ( key instanceof String ) {
      Object previous = get( key );
      propertiesConfiguration.setProperty( (String) key, value );
      return previous;
    }
    throw new IllegalArgumentException( "Can only store properties with String keys" );
  }

  private Set<String> getPropertyNames() {
    Set<String> result = new HashSet<>();
    Iterator<String> keys = propertiesConfiguration.getKeys();
    while ( keys.hasNext() ) {
      result.add( keys.next() );
    }
    return result;
  }

  @Override public synchronized Set<String> stringPropertyNames() {
    return Collections.unmodifiableSet( getPropertyNames() );
  }

  @Override public synchronized Set<Object> keySet() {
    return Collections.<Object>unmodifiableSet( getPropertyNames() );
  }

  private Map<String, Object> toMap() {
    Map<String, Object> result = new HashMap<>();
    Iterator<String> keys = propertiesConfiguration.getKeys();
    while ( keys.hasNext() ) {
      String next = keys.next();
      result.put( next, propertiesConfiguration.getProperty( next ) );
    }
    return result;
  }

  @Override public synchronized Set<Map.Entry<Object, Object>> entrySet() {
    Map map = toMap();
    return Collections.<Map.Entry<Object, Object>>unmodifiableSet( map.entrySet() );
  }

  @Override public synchronized int size() {
    return getPropertyNames().size();
  }

  @Override public synchronized boolean isEmpty() {
    return size() == 0;
  }

  @Override public synchronized Enumeration<Object> keys() {
    return new Vector( getPropertyNames() ).elements();
  }

  @Override public synchronized Enumeration<Object> elements() {
    return new Vector( toMap().values() ).elements();
  }

  @Override public synchronized boolean contains( Object value ) {
    return containsValue( value );
  }

  @Override public synchronized boolean containsValue( Object value ) {
    return values().contains( value );
  }

  @Override public synchronized Enumeration<?> propertyNames() {
    return new Vector( getPropertyNames() ).elements();
  }

  @Override public synchronized Collection<Object> values() {
    return toMap().values();
  }

  @Override public synchronized boolean containsKey( Object key ) {
    if ( key == null || key instanceof String ) {
      return propertiesConfiguration.containsKey( (String) key );
    }
    return false;
  }

  @Override public synchronized Object remove( Object key ) {
    if ( key == null || key instanceof String ) {
      Object result = propertiesConfiguration.getProperty( (String) key );
      propertiesConfiguration.clearProperty( (String) key );
      return result;
    }
    return null;
  }

  @Override public synchronized void putAll( Map<?, ?> t ) {
    for ( Map.Entry<?, ?> entry : t.entrySet() ) {
      put( entry.getKey(), entry.getValue() );
    }
  }

  @Override public synchronized void clear() {
    propertiesConfiguration.clear();
  }

  @Override public synchronized void load( Reader reader ) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override public synchronized void load( InputStream inStream ) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override public void save( OutputStream out, String comments ) {
    throw new UnsupportedOperationException();
  }

  @Override public void store( Writer writer, String comments ) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override public void store( OutputStream out, String comments ) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override public synchronized void loadFromXML( InputStream in )
    throws IOException, InvalidPropertiesFormatException {
    throw new UnsupportedOperationException();
  }

  @Override public void storeToXML( OutputStream os, String comment ) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override public void storeToXML( OutputStream os, String comment, String encoding ) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override public void list( PrintStream out ) {
    throw new UnsupportedOperationException();
  }

  @Override public void list( PrintWriter out ) {
    throw new UnsupportedOperationException();
  }

  @Override protected void rehash() {
    throw new UnsupportedOperationException();
  }

  @Override public synchronized Object clone() {
    throw new UnsupportedOperationException();
  }
}
