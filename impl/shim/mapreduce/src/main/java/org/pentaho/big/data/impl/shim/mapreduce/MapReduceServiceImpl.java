/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
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

package org.pentaho.big.data.impl.shim.mapreduce;

import com.google.common.annotations.VisibleForTesting;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.bigdata.api.mapreduce.MapReduceExecutionException;
import org.pentaho.bigdata.api.mapreduce.MapReduceJarInfo;
import org.pentaho.bigdata.api.mapreduce.MapReduceJobBuilder;
import org.pentaho.bigdata.api.mapreduce.MapReduceJobSimple;
import org.pentaho.bigdata.api.mapreduce.MapReduceService;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.job.entries.hadoopjobexecutor.JarUtility;
import org.pentaho.hadoop.shim.spi.HadoopShim;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Created by bryan on 12/1/15.
 */
public class MapReduceServiceImpl implements MapReduceService {
  public static final Class<?> PKG = MapReduceServiceImpl.class;
  public static final String ERROR_DRIVER_CLASS_NOT_SPECIFIED = "ErrorDriverClassNotSpecified";
  public static final String ERROR_MULTIPLE_DRIVER_CLASSES = "ErrorMultipleDriverClasses";
  private final NamedCluster namedCluster;
  private final HadoopShim hadoopShim;
  private final ExecutorService executorService;
  private final JarUtility jarUtility;

  public MapReduceServiceImpl( NamedCluster namedCluster, HadoopShim hadoopShim, ExecutorService executorService ) {
    this( namedCluster, hadoopShim, executorService, new JarUtility() );
  }

  public MapReduceServiceImpl( NamedCluster namedCluster, HadoopShim hadoopShim, ExecutorService executorService, JarUtility jarUtility ) {
    this.namedCluster = namedCluster;
    this.hadoopShim = hadoopShim;
    this.executorService = executorService;
    this.jarUtility = jarUtility;
  }

  @Override
  public MapReduceJobSimple executeSimple( URL resolvedJarUrl, String driverClass, final String commandLineArgs )
    throws MapReduceExecutionException {
    final Class<?> mainClass = locateDriverClass( driverClass, resolvedJarUrl, hadoopShim );
    return new FutureMapReduceJobSimpleImpl( executorService, mainClass, commandLineArgs );
  }

  @Override public MapReduceJobBuilder createJobBuilder( final LogChannelInterface log, VariableSpace variableSpace ) {
    return new MapReduceJobBuilderImpl( namedCluster, hadoopShim, log, variableSpace );
  }

  @Override public MapReduceJarInfo getJarInfo( URL resolvedJarUrl ) throws IOException, ClassNotFoundException {
    ClassLoader classLoader = getClass().getClassLoader();
    List<Class<?>> classesInJarWithMain =
      jarUtility.getClassesInJarWithMain( resolvedJarUrl.toExternalForm(), classLoader );
    List<String> classNamesInJarWithMain = new ArrayList<>( classesInJarWithMain.size() );
    for ( Class<?> aClass : classesInJarWithMain ) {
      classNamesInJarWithMain.add( aClass.getCanonicalName() );
    }
    classNamesInJarWithMain = Collections.unmodifiableList( classNamesInJarWithMain );

    final List<String> finalClassNamesInJarWithMain = classNamesInJarWithMain;

    final Class<?> mainClassFromManifest = jarUtility.getMainClassFromManifest( resolvedJarUrl, classLoader );

    return new MapReduceJarInfo() {
      @Override public List<String> getClassesWithMain() {
        return finalClassNamesInJarWithMain;
      }

      @Override public String getMainClass() {
        return mainClassFromManifest != null ? mainClassFromManifest.getCanonicalName() : null;
      }
    };
  }

  @VisibleForTesting
  Class<?> locateDriverClass( String driverClass, final URL resolvedJarUrl, final HadoopShim shim )
    throws MapReduceExecutionException {
    try {
      if ( Const.isEmpty( driverClass ) ) {
        Class<?> mainClass = jarUtility.getMainClassFromManifest( resolvedJarUrl, shim.getClass().getClassLoader() );
        if ( mainClass == null ) {
          List<Class<?>> mainClasses =
            jarUtility.getClassesInJarWithMain( resolvedJarUrl.toExternalForm(), shim.getClass().getClassLoader() );
          if ( mainClasses.size() == 1 ) {
            return mainClasses.get( 0 );
          } else if ( mainClasses.isEmpty() ) {
            throw new MapReduceExecutionException( BaseMessages.getString( PKG, ERROR_DRIVER_CLASS_NOT_SPECIFIED ) );
          } else {
            throw new MapReduceExecutionException( BaseMessages.getString( PKG, ERROR_MULTIPLE_DRIVER_CLASSES ) );
          }
        }
        return mainClass;
      } else {
        return jarUtility.getClassByName( driverClass, resolvedJarUrl, shim.getClass().getClassLoader() );
      }
    } catch ( Exception e ) {
      if ( e instanceof MapReduceExecutionException ) {
        throw (MapReduceExecutionException) e;
      } else {
        throw new MapReduceExecutionException( e );
      }
    }
  }
}
