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


package org.pentaho.di.job.entries.spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.pentaho.di.core.CheckResultInterface;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.pentaho.di.job.entries.spark.JobEntrySparkSubmit.JOB_TYPE_JAVA_SCALA;
import static org.pentaho.di.job.entries.spark.JobEntrySparkSubmit.JOB_TYPE_PYTHON;
import org.pentaho.di.job.JobMeta;

public class JobEntrySparkSubmitTest {
  @Test
  public void testGetCmds() throws IOException {
    JobEntrySparkSubmit ss = new JobEntrySparkSubmit();
    ss.setScriptPath( "scriptPath" );
    ss.setMaster( "master_url" );
    ss.setJobType( JOB_TYPE_JAVA_SCALA );
    ss.setJar( "jar_path" );
    ss.setArgs( "arg1 arg2" );
    ss.setClassName( "class_name" );
    ss.setDriverMemory( "driverMemory" );
    ss.setExecutorMemory( "executorMemory" );
    ss.setParentJobMeta( new JobMeta() );

    List<String> configParams = new ArrayList<String>();
    configParams.add( "name1=value1" );
    configParams.add( "name2=value 2" );
    ss.setConfigParams( configParams );

    Map<String, String> libs = new LinkedHashMap<>();
    libs.put("file:///path/to/lib1", "Local");
    libs.put("/path/to/lib2", "<Static>");
    ss.setLibs( libs );

    String[] expected = new String[] { "scriptPath", "--master", "master_url", "--conf", "name1=value1", "--conf",
        "name2=value 2", "--driver-memory", "driverMemory", "--executor-memory",
        "executorMemory", "--class", "class_name", "--jars", "file:///path/to/lib1,/path/to/lib2",  "jar_path", "arg1", "arg2" };
    Assert.assertArrayEquals( expected, ss.getCmds().toArray() );

    ss.setJobType( JOB_TYPE_PYTHON );
    ss.setPyFile( "pyFile-path" );
    expected = new String[] { "scriptPath", "--master", "master_url", "--conf", "name1=value1", "--conf",
        "name2=value 2", "--driver-memory", "driverMemory", "--executor-memory",
        "executorMemory", "--py-files", "file:///path/to/lib1,/path/to/lib2",  "pyFile-path", "arg1", "arg2" };
    Assert.assertArrayEquals( expected, ss.getCmds().toArray() );
  }

  @Test
  public void testValidate () {
    JobEntrySparkSubmit ss = spy( new JobEntrySparkSubmit() );
    doNothing().when( ss ).logError( anyString() );
    Assert.assertFalse( ss.validate() );
    // Use working dir which exists
    ss.setScriptPath( "." );
    ss.setMaster( "" );
    Assert.assertFalse( ss.validate() );
    ss.setMaster( "master-url" );
    Assert.assertFalse( "Jar path", ss.validate() );
    ss.setJobType( JOB_TYPE_JAVA_SCALA );
    Assert.assertFalse( "Jar path should not be empty", ss.validate() );
    ss.setJar( "jar-path" );
    Assert.assertTrue( "Validation should pass", ss.validate() );
    ss.setJobType( JobEntrySparkSubmit.JOB_TYPE_PYTHON );
    Assert.assertFalse( "Pyfile path should not be empty", ss.validate() );
    ss.setPyFile( "pyfile-path" );
    Assert.assertTrue( "Validation should pass", ss.validate() );
  }

  @Test
  public void testArgsParsing() throws IOException {
    JobEntrySparkSubmit ss = new JobEntrySparkSubmit();
    ss.setArgs( "${VAR1} \"double quoted string\" 'single quoted string'" );
    ss.setVariable( "VAR1", "VAR_VALUE" );
    List<String> cmds = ss.getCmds();
    Assert.assertTrue( cmds.containsAll( Arrays.asList( "VAR_VALUE", "double quoted string", "single quoted string" ) ) );
    Assert.assertFalse( cmds.contains( "${VAR1}" ) );
  }

  @Test
  public void testCheck() {
    JobEntrySparkSubmit je = new JobEntrySparkSubmit( "SparkSubmit" );
    je.setJobType( JOB_TYPE_JAVA_SCALA );
    List<CheckResultInterface> remarks = new ArrayList<>();
    je.setMaster( "" );
    je.check( remarks, new JobMeta(), null, null, null );
    Assert.assertEquals( "Number of remarks should be 4", 4, remarks.size() );

    int errors = 0;
    for ( CheckResultInterface remark : remarks ) {
      if ( remark.getType() == CheckResultInterface.TYPE_RESULT_ERROR ) {
        errors++;
      }
    }
    Assert.assertEquals( "Number of errors should be 4", 4, errors );

    remarks.clear();
    je.setJobType( JobEntrySparkSubmit.JOB_TYPE_PYTHON );
    je.check( remarks, new JobMeta(), null, null, null );
    Assert.assertEquals( "Number of remarks should be 4", 4, remarks.size() );

    errors = 0;
    for ( CheckResultInterface remark : remarks ) {
      if ( remark.getType() == CheckResultInterface.TYPE_RESULT_ERROR ) {
        errors++;
      }
    }
    Assert.assertEquals( "Number of errors should be 4", 4, errors );
  }
}
