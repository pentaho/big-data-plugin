/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2018 by Hitachi Vantara : http://www.pentaho.com
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

package org.pentaho.di.job.entries.spark;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.dictionary.MetaverseAnalyzers;
import org.pentaho.metaverse.api.IMetaverseNode;
import org.pentaho.metaverse.api.MetaverseAnalyzerException;
import org.pentaho.metaverse.api.analyzer.kettle.jobentry.JobEntryAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * A  data lineage analyzer for the "Spark Submit" job entry.
 */
public class JobEntrySparkSubmitAnalyzer extends JobEntryAnalyzer<JobEntrySparkSubmit> {

  private static final String CLASS_NAME = "className";
  private static final String PY_FILE = "pyFile";
  private static final String ARGUMENTS = "arguments";
  private static final String EXEC_MEMORY = "executorMemory";
  private static final String DRIVER_MEMORY = "driverMemory";
  private static final String MASTER_URL = "masterUrl";

  private Logger log = LoggerFactory.getLogger( JobEntrySparkSubmitAnalyzer.class );

  @Override
  public Set<Class<? extends JobEntryInterface>> getSupportedEntries() {
    Set<Class<? extends JobEntryInterface>> supportedEntries = new HashSet<Class<? extends JobEntryInterface>>();
    supportedEntries.add( JobEntrySparkSubmit.class );
    return supportedEntries;
  }

  @Override
  protected void customAnalyze( JobEntrySparkSubmit entry, IMetaverseNode rootNode ) throws MetaverseAnalyzerException {
    // -- Common properties
    rootNode.setProperty( ARGUMENTS, entry.environmentSubstitute( entry.getArgs() ) );
    rootNode.setProperty( EXEC_MEMORY, entry.environmentSubstitute( entry.getExecutorMemory() ) );
    rootNode.setProperty( DRIVER_MEMORY, entry.environmentSubstitute( entry.getDriverMemory() ) );
    rootNode.setProperty( MASTER_URL, entry.environmentSubstitute( entry.getMaster() ) );
    if ( JobEntrySparkSubmit.JOB_TYPE_JAVA_SCALA.equals( entry.getJobType() ) ) {
      // --- Java / Scala properties
      rootNode.setProperty( CLASS_NAME, entry.environmentSubstitute( entry.getClassName() ) );
      if ( StringUtils.isNotBlank( entry.getJar() ) ) {
        rootNode.setProperty( MetaverseAnalyzers.JobEntrySparkSubmitAnalyzer.APPLICATION_JAR,
          normalizePath( entry.environmentSubstitute( entry.getJar() ) ) );
      }
    } else if ( JobEntrySparkSubmit.JOB_TYPE_PYTHON.equals( entry.getJobType() ) ) {
      // Python properties
      if ( StringUtils.isNotBlank( entry.getPyFile() ) ) {
        rootNode.setProperty( MetaverseAnalyzers.JobEntrySparkSubmitAnalyzer.APPLICATION_JAR,
          normalizePath( entry.environmentSubstitute( entry.getPyFile() ) ) );
      }
    }
  }

  private String normalizePath( final String path ) {
    if ( StringUtils.isNotBlank( path ) ) {
      return path.replaceAll( "/", StringEscapeUtils.escapeJava( "/" ) )
        .replaceAll(  "\\\\", StringEscapeUtils.escapeJava( "/" ) );
    }
    return path;
  }
}
