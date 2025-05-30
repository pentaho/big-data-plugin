/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package com.pentaho.big.data.bundles.impl.shim.common;

import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.wiring.BundleWiring;
import org.pentaho.di.core.bowl.DefaultBowl;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.plugins.LifecyclePluginType;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.vfs.KettleVFS;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by bryan on 10/5/15.
 *
 * @deprecated
 */
@Deprecated
public class ShimBridgingClassloaderTest {
  private PluginClassloaderGetter originalPluginClassloaderGetter;
  private PluginClassloaderGetter pluginClassloaderGetter;
  private ShimBridgingClassloader.PublicLoadResolveClassLoader parentClassLoader;
  private BundleContext bundleContext;
  private ShimBridgingClassloader shimBridgingClassloader;
  private BundleWiring bundleWiring;
  private Bundle bundle;
  private ShimBridgingClassloader.PublicLoadResolveClassLoader bundleWiringClassloader;

  @Before
  public void setup() {
    parentClassLoader = mock( ShimBridgingClassloader.PublicLoadResolveClassLoader.class );
    bundleContext = mock( BundleContext.class );
    bundleWiring = mock( BundleWiring.class );
    bundle = mock( Bundle.class );
    bundleWiringClassloader = mock( ShimBridgingClassloader.PublicLoadResolveClassLoader.class );
    when( bundleWiring.getClassLoader() ).thenReturn( bundleWiringClassloader );
    when( bundleContext.getBundle() ).thenReturn( bundle );
    when( bundle.adapt( BundleWiring.class ) ).thenReturn( bundleWiring );
    shimBridgingClassloader = new ShimBridgingClassloader( parentClassLoader, bundleContext );
    originalPluginClassloaderGetter = ShimBridgingClassloader.getPluginClassloaderGetter();
    pluginClassloaderGetter = mock( PluginClassloaderGetter.class );
    ShimBridgingClassloader.setPluginClassloaderGetter( pluginClassloaderGetter );
  }

  @After
  public void teardown() {
    ShimBridgingClassloader.setPluginClassloaderGetter( originalPluginClassloaderGetter );
  }

  @Test
  public void testCreateSuccessWithArgs()
    throws ClassNotFoundException, InvocationTargetException, InstantiationException, KettlePluginException,
    IllegalAccessException, KettleFileException, IOException {
    String testName = "testName";

    String canonicalName = ValueMetaInteger.class.getCanonicalName();
    FileObject myFile = KettleVFS.getInstance( DefaultBowl.getInstance() )
      .getFileObject( "ram://testCreateSuccessWithArgs" );
    try ( FileObject fileObject = myFile ) {
      try ( OutputStream outputStream = fileObject.getContent().getOutputStream( false ) ) {
        IOUtils.copy( getClass().getClassLoader().getResourceAsStream(
          canonicalName.replace( ".", "/" ) + ".class" ), outputStream );
      }
      String packageName = ValueMetaInteger.class.getPackage().getName();
      when( bundleWiring.findEntries( "/" + packageName.replace( ".", "/" ),
        ValueMetaInteger.class.getSimpleName() + ".class", 0 ) )
        .thenReturn( Arrays.asList( fileObject.getURL() ) );
      when( parentClassLoader.loadClass( anyString(), anyBoolean() ) ).thenAnswer( new Answer<Class<?>>() {
        @Override public Class<?> answer( InvocationOnMock invocation ) throws Throwable {
          Object[] arguments = invocation.getArguments();
          return new ShimBridgingClassloader.PublicLoadResolveClassLoader( getClass().getClassLoader() )
            .loadClass( (String) arguments[ 0 ], (boolean) arguments[ 1 ] );
        }
      } );
      when( pluginClassloaderGetter.getPluginClassloader( LifecyclePluginType.class.getCanonicalName(),
        ShimBridgingClassloader.HADOOP_SPOON_PLUGIN ) ).thenReturn( parentClassLoader );
      // With name
      Object o = ShimBridgingClassloader
        .create( bundleContext, canonicalName, Arrays.<Object>asList( testName ) );
      // Interface should be same class object
      assertTrue( o instanceof ValueMetaInterface );
      assertEquals( testName, ( (ValueMetaInterface) o ).getName() );
      // Shouldn't be true because it was loaded from diff classloader
      assertFalse( o instanceof ValueMetaInteger );

      // Null argument
      ArrayList<Object> arguments = new ArrayList<>();
      arguments.add( null );
      o = ShimBridgingClassloader.create( bundleContext, canonicalName, arguments );
      // Interface should be same class object
      assertTrue( o instanceof ValueMetaInterface );
      assertNull( ( (ValueMetaInterface) o ).getName() );
      // Shouldn't be true because it was loaded from diff classloader
      assertFalse( o instanceof ValueMetaInteger );

      // Null arg list
      o = ShimBridgingClassloader.create( bundleContext, canonicalName, null );
      // Interface should be same class object
      assertTrue( o instanceof ValueMetaInterface );
      assertNull( ( (ValueMetaInterface) o ).getName() );
      // Shouldn't be true because it was loaded from diff classloader
      assertFalse( o instanceof ValueMetaInteger );

      // Empty arg list
      o = ShimBridgingClassloader.create( bundleContext, canonicalName, new ArrayList<Object>() );
      // Interface should be same class object
      assertTrue( o instanceof ValueMetaInterface );
      assertNull( ( (ValueMetaInterface) o ).getName() );
      // Shouldn't be true because it was loaded from diff classloader
      assertFalse( o instanceof ValueMetaInteger );
    } finally {
      myFile.delete();
    }
  }

  @Test( expected = InstantiationException.class )
  public void testCreateFalureNoMatching()
    throws ClassNotFoundException, InvocationTargetException, InstantiationException, KettlePluginException,
    IllegalAccessException, KettleFileException, IOException {
    String testName = "testName";

    String canonicalName = ValueMetaInteger.class.getCanonicalName();
    FileObject myFile = KettleVFS.getInstance( DefaultBowl.getInstance() )
      .getFileObject( "ram://testCreateSuccessWithArgs" );
    try ( FileObject fileObject = myFile ) {
      try ( OutputStream outputStream = fileObject.getContent().getOutputStream( false ) ) {
        IOUtils.copy( getClass().getClassLoader().getResourceAsStream(
          canonicalName.replace( ".", "/" ) + ".class" ), outputStream );
      }
      String packageName = ValueMetaInteger.class.getPackage().getName();
      when( bundleWiring.findEntries( "/" + packageName.replace( ".", "/" ),
        ValueMetaInteger.class.getSimpleName() + ".class", 0 ) )
        .thenReturn( Arrays.asList( fileObject.getURL() ) );
      when( parentClassLoader.loadClass( anyString(), anyBoolean() ) ).thenAnswer( new Answer<Class<?>>() {
        @Override public Class<?> answer( InvocationOnMock invocation ) throws Throwable {
          Object[] arguments = invocation.getArguments();
          return new ShimBridgingClassloader.PublicLoadResolveClassLoader( getClass().getClassLoader() )
            .loadClass( (String) arguments[ 0 ], (boolean) arguments[ 1 ] );
        }
      } );
      when( pluginClassloaderGetter.getPluginClassloader( LifecyclePluginType.class.getCanonicalName(),
        ShimBridgingClassloader.HADOOP_SPOON_PLUGIN ) ).thenReturn( parentClassLoader );
      Object o = ShimBridgingClassloader.create( bundleContext, canonicalName, Arrays.<Object>asList( 1.1 ) );
    } finally {
      myFile.delete();
    }
  }

  @Test
  public void testFindClassSuccess() throws ClassNotFoundException, IOException, KettleFileException {
    String canonicalName = ShimBridgingClassloader.class.getCanonicalName();
    FileObject myFile = KettleVFS.getInstance( DefaultBowl.getInstance() )
      .getFileObject( "ram://testFindClassSuccess" );
    try ( FileObject fileObject = myFile ) {
      try ( OutputStream outputStream = fileObject.getContent().getOutputStream( false ) ) {
        IOUtils.copy( getClass().getClassLoader().getResourceAsStream(
          canonicalName.replace( ".", "/" ) + ".class" ), outputStream );
      }
      String packageName = ShimBridgingClassloader.class.getPackage().getName();
      when( bundleWiring.findEntries( "/" + packageName.replace( ".", "/" ),
        ShimBridgingClassloader.class.getSimpleName() + ".class", 0 ) )
        .thenReturn( Arrays.asList( fileObject.getURL() ) );
      when( parentClassLoader.loadClass( anyString(), anyBoolean() ) ).thenAnswer( new Answer<Class<?>>() {
        @Override public Class<?> answer( InvocationOnMock invocation ) throws Throwable {
          Object[] arguments = invocation.getArguments();
          return new ShimBridgingClassloader.PublicLoadResolveClassLoader( getClass().getClassLoader() )
            .loadClass( (String) arguments[ 0 ], (boolean) arguments[ 1 ] );
        }
      } );
      Class<?> shimBridgingClassloaderClass = shimBridgingClassloader.findClass( canonicalName );
      assertEquals( canonicalName, shimBridgingClassloaderClass.getCanonicalName() );
      assertEquals( shimBridgingClassloader, shimBridgingClassloaderClass.getClassLoader() );
      assertEquals( packageName, shimBridgingClassloaderClass.getPackage().getName() );
    } finally {
      myFile.delete();
    }
  }

  @Test( expected = ClassNotFoundException.class )
  public void testFindFailReading() throws ClassNotFoundException, IOException, KettleFileException {
    FileObject myFile = KettleVFS.getInstance( DefaultBowl.getInstance() ).getFileObject( "ram://testFindFailReading" );
    try ( FileObject fileObject = myFile ) {
      when( bundleWiring.findEntries( "/" + ShimBridgingClassloader.class.getPackage().getName().replace( ".", "/" ),
        ShimBridgingClassloader.class.getSimpleName() + ".class", 0 ) )
        .thenReturn( Arrays.asList( fileObject.getURL() ) );
      shimBridgingClassloader.findClass( ShimBridgingClassloader.class.getCanonicalName() );
    } finally {
      myFile.delete();
    }
  }

  @Test( expected = ClassNotFoundException.class )
  public void testFindNotFound() throws ClassNotFoundException {
    when( bundleWiring.findEntries( "/" + ShimBridgingClassloader.class.getPackage().getName().replace( ".", "/" ),
      ShimBridgingClassloader.class.getSimpleName() + ".class", 0 ) )
      .thenReturn( Arrays.<URL>asList() );
    shimBridgingClassloader.findClass( ShimBridgingClassloader.class.getCanonicalName() );
  }

  @Test
  public void testGetResourceDefaultPackage() throws MalformedURLException {
    String testName = "testName";
    URL url = new URL( "file://testGetResourceDefaultPackage" );
    when( bundleWiring.findEntries( "/", testName, 0 ) ).thenReturn( Arrays.asList( url ) );
    assertEquals( url, shimBridgingClassloader.getResource( testName ) );
  }

  @Test
  public void testGetResourceInPackage() throws MalformedURLException {
    String path = "testPath";
    String testName = "testName";
    URL url = new URL( "file://path/testGetResourceDefaultPackage" );
    when( bundleWiring.findEntries( "/testPath", testName, 0 ) ).thenReturn( Arrays.asList( url ) );
    assertEquals( url, shimBridgingClassloader.getResource( "/" + path + "/" + testName ) );
  }

  @Test
  public void testGetResourceBundleWiringClassloader() throws MalformedURLException {
    String testName = "testName";
    URL url = new URL( "file://testGetResourceBundleWiringClassloader" );
    when( bundleWiringClassloader.getResource( testName ) ).thenReturn( url );
    assertEquals( url, shimBridgingClassloader.getResource( testName ) );
  }

  @Test
  public void testGetResourceParentClassloader() throws MalformedURLException {
    String testName = "testName";
    URL url = new URL( "file://testGetResourceParentClassloader" );
    when( parentClassLoader.getResource( testName ) ).thenReturn( url );
    assertEquals( url, shimBridgingClassloader.getResource( testName ) );
  }

  @Test
  public void testLoadClassFromBothOSGiAndParent() throws ClassNotFoundException {
    String testName = "testName";
    when( parentClassLoader.loadClass( testName, false ) ).thenReturn( (Class) Object.class );
    when( bundleWiringClassloader.loadClass( testName, false ) ).thenReturn( (Class) PluginRegistry.class );
    assertEquals( Object.class, shimBridgingClassloader.loadClass( testName ) );
  }

  @Test
  public void testLoadClassFromBothOSGiAndParentParentException() throws ClassNotFoundException {
    String testName = "testName";
    when( parentClassLoader.loadClass( testName, false ) ).thenThrow( new RuntimeException() );
    when( bundleWiringClassloader.loadClass( testName, false ) ).thenReturn( (Class) PluginRegistry.class );
    assertEquals( PluginRegistry.class, shimBridgingClassloader.loadClass( testName ) );
  }

  @Test
  public void testLoadClassFromOSGi() throws ClassNotFoundException {
    String testName = "testName";
    when( bundleWiringClassloader.loadClass( testName, false ) ).thenReturn( (Class) Object.class );
    assertEquals( Object.class, shimBridgingClassloader.loadClass( testName ) );
  }

  @Test
  public void testLoadClassFromParent() throws ClassNotFoundException {
    String testName = "testName";
    when( parentClassLoader.loadClass( testName, false ) ).thenReturn( (Class) Object.class );
    assertEquals( Object.class, shimBridgingClassloader.loadClass( testName ) );
  }

  @Test
  public void testLoadClassFindClass() throws ClassNotFoundException, IOException, KettleFileException {
    String canonicalName = ShimBridgingClassloader.class.getCanonicalName();
    FileObject myFile = KettleVFS.getInstance( DefaultBowl.getInstance() )
      .getFileObject( "ram://testLoadClassFindClass" );
    try ( FileObject fileObject = myFile ) {
      try ( OutputStream outputStream = fileObject.getContent().getOutputStream( false ) ) {
        IOUtils.copy( getClass().getClassLoader().getResourceAsStream(
          canonicalName.replace( ".", "/" ) + ".class" ), outputStream );
      }
      String packageName = ShimBridgingClassloader.class.getPackage().getName();
      when( bundleWiring.findEntries( "/" + packageName.replace( ".", "/" ),
        ShimBridgingClassloader.class.getSimpleName() + ".class", 0 ) )
        .thenReturn( Arrays.asList( fileObject.getURL() ) );
      when( parentClassLoader.loadClass( anyString(), anyBoolean() ) ).thenAnswer( new Answer<Class<?>>() {
        @Override public Class<?> answer( InvocationOnMock invocation ) throws Throwable {
          Object[] arguments = invocation.getArguments();
          return new ShimBridgingClassloader.PublicLoadResolveClassLoader( getClass().getClassLoader() )
            .loadClass( (String) arguments[ 0 ], (boolean) arguments[ 1 ] );
        }
      } );
      Class<?> shimBridgingClassloaderClass = shimBridgingClassloader.loadClass( canonicalName, true );
      assertEquals( canonicalName, shimBridgingClassloaderClass.getCanonicalName() );
      assertEquals( shimBridgingClassloader, shimBridgingClassloaderClass.getClassLoader() );
      assertEquals( packageName, shimBridgingClassloaderClass.getPackage().getName() );
      Class<?> shimBridgingClassloaderClass2 = shimBridgingClassloader.loadClass( canonicalName, false );
      assertEquals( shimBridgingClassloaderClass, shimBridgingClassloaderClass2 );
    } finally {
      myFile.delete();
    }
  }

  /*
  BACKLOG-19039 - Yarn step for secure cluster stop working after license verifier check
  Bug reason - license verifier tries to load bundle for com.pentaho.yarn.impl.shim.YarnServiceImpl.class using ShimBridgingClassloader
  for getting bundle header Implementation-Version from bundle info and can't do it because ShimBridgingClassloader doesn't implement interface BundleReference
  As a solution added implementation of interface BundleReference to ShimBridgingClassloader
  Test possibility of ShimBridgingClassloader to return bundle, when it invokes from FrameworkUtil.getBundle(Class<?>)
   */
  @Test
  public void testGetBundleWhenRequestingBundleShouldReturnBundle() throws ClassNotFoundException, IOException, KettleFileException{
    String canonicalName = ShimBridgingClassloader.class.getCanonicalName();
    String packageName = ShimBridgingClassloader.class.getPackage().getName();
    URL url = getClass().getClassLoader().getResource(canonicalName.replace( ".", "/" ) + ".class");
    when( bundleWiring.findEntries( "/" + packageName.replace( ".", "/" ),
            ShimBridgingClassloader.class.getSimpleName() + ".class", 0 ) )
            .thenReturn( Arrays.asList( url ) );
    when( parentClassLoader.loadClass( anyString(), anyBoolean() ) ).thenAnswer( new Answer<Class<?>>() {
      @Override public Class<?> answer( InvocationOnMock invocation ) throws Throwable {
        Object[] arguments = invocation.getArguments();
        return new ShimBridgingClassloader.PublicLoadResolveClassLoader( getClass().getClassLoader() )
                .loadClass( (String) arguments[ 0 ], (boolean) arguments[ 1 ] );
      }
    } );
    Class<?> shimBridgingClassloaderClass = shimBridgingClassloader.loadClass( canonicalName, true );
    when(shimBridgingClassloader.getBundle()).thenReturn(bundle);

    Bundle actualBundle = FrameworkUtil.getBundle(shimBridgingClassloaderClass);

    assertEquals(actualBundle, bundle);
  }
}
