/*! ******************************************************************************
 *
 * Pentaho Data Integration
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

package org.pentaho.di.job.entries.spark;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class JobEntrySparkSubmitTest {
  @Test
  public void testGetCmds() throws IOException {
    JobEntrySparkSubmit ss = new JobEntrySparkSubmit();
    ss.setScriptPath( "scriptPath" );
    ss.setMaster( "master_url" );
    ss.setJar( "jar_path" );
    ss.setArgs( "arg1 arg2" );
    ss.setClassName( "class_name" );
    ss.setDriverMemory( "driverMemory" );
    ss.setExecutorMemory( "executorMemory" );

    List<String> configParams = new ArrayList<String>();
    configParams.add( "name1=value1" );
    configParams.add( "name2=value 2" );
    ss.setConfigParams( configParams );

    String[] expected = new String[] { "scriptPath", "--master", "master_url", "--class", "class_name",
        "--conf", "name1=value1", "--conf", "name2=value 2", "--driver-memory", "driverMemory", "--executor-memory", "executorMemory", "jar_path", "arg1", "arg2" };
    Assert.assertArrayEquals( expected, ss.getCmds().toArray() );
  }

  @Test
  public void testValidate () {
    JobEntrySparkSubmit ss = spy( new JobEntrySparkSubmit() );
    doNothing().when( ss ).logError( anyString() );
    Assert.assertFalse( ss.validate() );
    // Use working dir which exists
    ss.setScriptPath( "." );
    Assert.assertFalse( ss.validate() );
    ss.setMaster( "master-url" );
    Assert.assertFalse( "Jar path", ss.validate() );
    ss.setJar( "jar-path" );
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
}
