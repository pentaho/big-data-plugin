/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.bigdata;

import java.io.File;
import java.lang.annotation.Annotation;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileSelector;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.plugins.JobEntryPluginType;
import org.pentaho.di.core.plugins.KettleURLClassLoader;
import org.pentaho.di.core.plugins.PluginAnnotationType;
import org.pentaho.di.core.plugins.PluginFolder;
import org.pentaho.di.core.plugins.PluginFolderInterface;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginMainClassType;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.PluginTypeCategoriesOrder;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entry.JobEntryInterface;

@PluginTypeCategoriesOrder( getNaturalCategoriesOrder = { "JobCategory.Category.General", "JobCategory.Category.Mail",
  "JobCategory.Category.FileManagement", "JobCategory.Category.Conditions", "JobCategory.Category.Scripting",
  "JobCategory.Category.BulkLoading", "JobCategory.Category.BigData", "JobCategory.Category.DataQuality",
  "JobCategory.Category.XML", "JobCategory.Category.Utility", "JobCategory.Category.Repository",
  "JobCategory.Category.FileTransfer", "JobCategory.Category.FileEncryption", "JobCategory.Category.Palo",
  "JobCategory.Category.Experimental", "JobCategory.Category.Deprecated" }, i18nPackageClass = JobMeta.class )
@PluginMainClassType( JobEntryInterface.class )
@PluginAnnotationType( JobEntry.class )
public class ShimDependentJobEntryPluginType extends JobEntryPluginType {
  private static final ShimDependentJobEntryPluginType instance = new ShimDependentJobEntryPluginType();
  private final Map<Set<String>, KettleURLClassLoader> classLoaderMap =
    new HashMap<Set<String>, KettleURLClassLoader>();

  private ShimDependentJobEntryPluginType() {
    super( ShimDependentJobEntry.class, "SHIM_DEPENDENT_JOBENTRY", "Shim Dependent Job entry" );
  }

  public static ShimDependentJobEntryPluginType getInstance() {
    return instance;
  }

  @Override
  public List<PluginFolderInterface> getPluginFolders() {
    return Arrays.<PluginFolderInterface>asList( new PluginFolder( new File( ShimDependentJobEntryPluginType.class
      .getProtectionDomain().getCodeSource().getLocation().getPath() ).getParentFile().toURI().toString()
      + "plugins/", false, true ) {
      @Override
      public FileObject[] findJarFiles( final boolean includeLibJars ) throws KettleFileException {
        try {
          // Find all the jar files in this folder...
          //
          FileObject folderObject = KettleVFS.getFileObject( this.getFolder() );
          FileObject[] fileObjects = folderObject.findFiles( new FileSelector() {
            @Override
            public boolean traverseDescendents( FileSelectInfo fileSelectInfo ) throws Exception {
              FileObject fileObject = fileSelectInfo.getFile();
              String folder = fileObject.getName().getBaseName();
              return includeLibJars || !"lib".equals( folder );
            }

            @Override
            public boolean includeFile( FileSelectInfo fileSelectInfo ) throws Exception {
              return fileSelectInfo.getFile().toString().endsWith( ".jar" );
            }
          } );

          return fileObjects;
        } catch ( Exception e ) {
          throw new KettleFileException( "Unable to list jar files in plugin folder '" + toString() + "'", e );
        }
      }
    } );
  }

  @Override
  public void handlePluginAnnotation( Class<?> clazz, Annotation annotation, List<String> libraries,
                                      boolean nativePluginType, URL pluginFolder ) throws KettlePluginException {
    String idList = extractID( annotation );
    if ( Const.isEmpty( idList ) ) {
      throw new KettlePluginException( "No ID specified for plugin with class: " + clazz.getName() );
    }

    // Only one ID for now
    String[] ids = idList.split( "," );
    super.handlePluginAnnotation( clazz, annotation, libraries, nativePluginType, pluginFolder );
    PluginInterface plugin =
      PluginRegistry.getInstance().findPluginWithId( ShimDependentJobEntryPluginType.class, ids[ 0 ] );
    URL[] urls = new URL[ libraries.size() ];
    for ( int i = 0; i < libraries.size(); i++ ) {
      File jarfile = new File( libraries.get( i ) );
      try {
        urls[ i ] = new URL( URLDecoder.decode( jarfile.toURI().toURL().toString(), "UTF-8" ) );
      } catch ( Exception e ) {
        throw new KettlePluginException( e );
      }
    }
    // try {
    Set<String> librarySet = new HashSet<String>( libraries );
    KettleURLClassLoader classloader = classLoaderMap.get( librarySet );
    //todo: what the ?? when multishim, any classloader needed to be put? or big-data-plugin classloader here
    //      if ( classloader == null ) {
    //        classloader =
    //            new KettleURLClassLoader( urls, HadoopConfigurationBootstrap.getHadoopConfigurationProvider()
    //                .getActiveConfiguration().getHadoopShim().getClass().getClassLoader() );
    //        classLoaderMap.put( librarySet, classloader );
    //      }
    //      PluginRegistry.getInstance().addClassLoader( classloader, plugin );
    //    } catch ( ConfigurationException e ) {
    //      throw new KettlePluginException( e );
    //    }
  }

  @Override
  protected void registerNatives() throws KettlePluginException {
    // noop
  }

  @Override
  protected String extractCategory( Annotation annotation ) {
    return ( (ShimDependentJobEntry) annotation ).categoryDescription();
  }

  @Override
  protected String extractDesc( Annotation annotation ) {
    return ( (ShimDependentJobEntry) annotation ).description();
  }

  @Override
  protected String extractID( Annotation annotation ) {
    return ( (ShimDependentJobEntry) annotation ).id();
  }

  @Override
  protected String extractName( Annotation annotation ) {
    return ( (ShimDependentJobEntry) annotation ).name();
  }

  @Override
  protected String extractImageFile( Annotation annotation ) {
    return ( (ShimDependentJobEntry) annotation ).image();
  }

  @Override
  protected String extractI18nPackageName( Annotation annotation ) {
    return ( (ShimDependentJobEntry) annotation ).i18nPackageName();
  }

  @Override
  protected String extractDocumentationUrl( Annotation annotation ) {
    return ( (ShimDependentJobEntry) annotation ).documentationUrl();
  }

  @Override
  protected String extractCasesUrl( Annotation annotation ) {
    return ( (ShimDependentJobEntry) annotation ).casesUrl();
  }

  @Override
  protected String extractForumUrl( Annotation annotation ) {
    return ( (ShimDependentJobEntry) annotation ).forumUrl();
  }
}
