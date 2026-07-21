/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



package org.pentaho.hadoop;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.ReloadingFileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
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
  private final ReloadingFileBasedConfigurationBuilder<PropertiesConfiguration> configurationBuilder;
  private final PropertiesConfiguration propertiesConfiguration;

  public PropertiesConfigurationProperties( FileObject fileObject ) throws ConfigurationException, FileSystemException {
    this.configurationBuilder = initConfigurationBuilder( fileObject );
    this.propertiesConfiguration = null;
  }

  public PropertiesConfigurationProperties( PropertiesConfiguration propertiesConfiguration ) {
    this.propertiesConfiguration = propertiesConfiguration;
    this.configurationBuilder = null;
  }

  private static ReloadingFileBasedConfigurationBuilder<PropertiesConfiguration> initConfigurationBuilder(
    FileObject fileObject ) throws ConfigurationException, FileSystemException {
    ReloadingFileBasedConfigurationBuilder<PropertiesConfiguration> builder =
      new ReloadingFileBasedConfigurationBuilder<>( PropertiesConfiguration.class, null, true )
        .configure( new Parameters().properties().setURL( fileObject.getURL() ) );
    builder.setAutoSave( true );
    // Trigger the initial load so any problem with the file surfaces here, as it did with the 1.x constructor.
    builder.getConfiguration();
    return builder;
  }

  private PropertiesConfiguration getPropertiesConfiguration() {
    if ( configurationBuilder == null ) {
      return propertiesConfiguration;
    }
    try {
      configurationBuilder.getReloadingController().checkForReloading( null );
      return configurationBuilder.getConfiguration();
    } catch ( ConfigurationException e ) {
      throw new IllegalStateException( e );
    }
  }

  @Override public synchronized String getProperty( String key ) {
    return getProperty( key, null );
  }

  @Override public synchronized String getProperty( String key, String defaultValue ) {
    return getPropertiesConfiguration().getString( key, defaultValue );
  }

  @Override public synchronized Object get( Object key ) {
    if ( key == null || key instanceof String ) {
      return getPropertiesConfiguration().getProperty( (String) key );
    } else {
      return null;
    }
  }

  @Override public synchronized Object setProperty( String key, String value ) {
    return put( key, value );
  }

  @Override public synchronized Object put( Object key, Object value ) {
    if ( key instanceof String ) {
      PropertiesConfiguration config = getPropertiesConfiguration();
      Object previous = config.getProperty( (String) key );
      config.setProperty( (String) key, value );
      return previous;
    }
    throw new IllegalArgumentException( "Can only store properties with String keys" );
  }

  private Set<String> getPropertyNames() {
    Set<String> result = new HashSet<>();
    Iterator<String> keys = getPropertiesConfiguration().getKeys();
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
    PropertiesConfiguration config = getPropertiesConfiguration();
    Iterator<String> keys = config.getKeys();
    while ( keys.hasNext() ) {
      String next = keys.next();
      result.put( next, config.getProperty( next ) );
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
      return getPropertiesConfiguration().containsKey( (String) key );
    }
    return false;
  }

  @Override public synchronized Object remove( Object key ) {
    if ( key == null || key instanceof String ) {
      PropertiesConfiguration config = getPropertiesConfiguration();
      Object result = config.getProperty( (String) key );
      config.clearProperty( (String) key );
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
    getPropertiesConfiguration().clear();
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
