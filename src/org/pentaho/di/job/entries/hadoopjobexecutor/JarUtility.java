/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2013 by Pentaho : http://www.pentaho.com
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

package org.pentaho.di.job.entries.hadoopjobexecutor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

/**
 * Utility class for working with Jar files in the context of configuring MapReduce jobs.
 */
public class JarUtility {

  /**
   * Get the Main-Class declaration from a Jar's manifest.
   * 
   * @param jarUrl
   *          URL to the Jar file
   * @param parentClassLoader
   *          Class loader to delegate to when loading classes outside the jar.
   * @return Class defined by Main-Class manifest attribute, {@code null} if not defined.
   * @throws IOException
   *           Error opening jar file
   * @throws ClassNotFoundException
   *           Error locating the main class as defined in the manifest
   */
  public Class<?> getMainClassFromManifest( URL jarUrl, ClassLoader parentClassLoader )
    throws IOException, ClassNotFoundException {
    JarFile jarFile = getJarFile( jarUrl, parentClassLoader );
    try {
      Manifest manifest = jarFile.getManifest();
      String className = manifest == null ? null : manifest.getMainAttributes().getValue( "Main-Class" );
      return loadClassByName( className, jarUrl, parentClassLoader );
    } finally {
      jarFile.close();
    }
  }

  /**
   * Load the specified class from the specified jar and return the Class object
   *
   * @param className
   *          Name of class to load
   * @param jarUrl
   *          URL to the Jar file
   * @param parentClassLoader
   *          Class loader to delegate to when loading classes outside the jar.
   * @return Class defined by className parameter, {@code null} if not defined.
   * @throws IOException
   *           Error opening jar file
   * @throws ClassNotFoundException
   *           Error locating the main class as defined in the manifest
   */
  public Class<?> getClassByName( String className, URL jarUrl, ClassLoader parentClassLoader )
    throws IOException, ClassNotFoundException {
    JarFile jarFile = getJarFile( jarUrl, parentClassLoader );
    try {
      return loadClassByName( className, jarUrl, parentClassLoader );
    } finally {
      jarFile.close();
    }
  }

  private Class<?> loadClassByName( final String className, final URL jarUrl, final ClassLoader parentClassLoader )
    throws ClassNotFoundException {
    if ( className != null ) {
      URLClassLoader cl = new URLClassLoader( new URL[] { jarUrl }, parentClassLoader );
      return cl.loadClass( className.replaceAll( "/", "." ) );
    } else {
      return null;
    }
  }

  private JarFile getJarFile( final URL jarUrl, final ClassLoader parentClassLoader ) throws IOException {
    if ( jarUrl == null || parentClassLoader == null ) {
      throw new NullPointerException();
    }
    JarFile jarFile;
    try {
      jarFile = new JarFile( new File( jarUrl.toURI() ) );
    } catch ( URISyntaxException ex ) {
      throw new IOException( "Error locating jar: " + jarUrl );
    } catch ( IOException ex ) {
      throw new IOException( "Error opening job jar: " + jarUrl, ex );
    }
    return jarFile;
  }

  public List<Class<?>> getClassesInJarWithMain( String jarUrl, ClassLoader parentClassloader )
    throws MalformedURLException {
    ArrayList<Class<?>> mainClasses = new ArrayList<Class<?>>();
    List<Class<?>> allClasses = JarUtility.getClassesInJar( jarUrl, parentClassloader );
    for ( Class<?> clazz : allClasses ) {
      try {
        Method mainMethod = clazz.getMethod( "main", new Class[] { String[].class } );
        if ( Modifier.isStatic( mainMethod.getModifiers() ) ) {
          mainClasses.add( clazz );
        }
      } catch ( Throwable ignored ) {
        // Ignore classes without main() methods
      }
    }
    return mainClasses;
  }

  public static List<Class<?>> getClassesInJar( String jarUrl, ClassLoader parentClassloader )
    throws MalformedURLException {

    URL url = new URL( jarUrl );
    URL[] urls = new URL[] { url };
    URLClassLoader loader = new URLClassLoader( urls, parentClassloader );

    ArrayList<Class<?>> classes = new ArrayList<Class<?>>();

    try {
      JarInputStream jarFile = new JarInputStream( new FileInputStream( new File( url.toURI() ) ) );
      JarEntry jarEntry;

      while ( true ) {
        jarEntry = jarFile.getNextJarEntry();
        if ( jarEntry == null ) {
          break;
        }
        if ( jarEntry.getName().endsWith( ".class" ) ) {
          String className =
              jarEntry.getName().substring( 0, jarEntry.getName().indexOf( ".class" ) ).replaceAll( "/", "\\." );
          classes.add( loader.loadClass( className ) );
        }
      }
    } catch ( Throwable e ) {
      e.printStackTrace();
    }
    return classes;
  }

}
