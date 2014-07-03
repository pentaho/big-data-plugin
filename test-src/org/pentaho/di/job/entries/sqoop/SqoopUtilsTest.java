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

package org.pentaho.di.job.entries.sqoop;

import java.beans.PropertyChangeEvent;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.job.ArgumentWrapper;
import org.pentaho.di.job.CommandLineArgument;
import org.pentaho.di.job.JobEntryMode;
import org.pentaho.di.job.entries.helper.PersistentPropertyChangeListener;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class SqoopUtilsTest {
  public static class MockConfig extends SqoopConfig {
    @CommandLineArgument( name = "test", displayName = "Test", description = "Test argument", flag = true )
    private String test;

    @CommandLineArgument( name = "strictlyNotEmpty" )
    private String strictlyNotEmpty;

    public String getTest() {
      return test;
    }

    public void setTest( String test ) {
      String old = this.test;
      this.test = test;
      pcs.firePropertyChange( "test", old, this.test );
    }

    public String getStrictlyNotEmpty() {
      return strictlyNotEmpty;
    }

    public void setStrictlyNotEmpty( String strictlyNotEmpty ) {
      String old = this.strictlyNotEmpty;
      this.strictlyNotEmpty = strictlyNotEmpty;
      pcs.firePropertyChange( "strictlyNotEmpty", old, this.strictlyNotEmpty );
    }
  }

  @Test
  public void getCommandLineArgs_empty() throws IOException {
    Variables v = new Variables();
    SqoopConfig config = new SqoopExportConfig();
    assertEquals( 0, SqoopUtils.getCommandLineArgs( config, v ).size() );

    // Job Entry Name is not annotated so it shouldn't be added to the args list
    config.setJobEntryName( "testing" );
    assertEquals( 0, SqoopUtils.getCommandLineArgs( config, v ).size() );
  }

  @Test
  public void getCommandLineArgs_boolean() throws IOException {
    Variables v = new Variables();
    SqoopConfig config = new SqoopExportConfig();

    config.setVerbose( Boolean.TRUE.toString() );

    List<String> args = SqoopUtils.getCommandLineArgs( config, v );
    assertEquals( 1, args.size() );
    assertEquals( "--verbose", args.get( 0 ) );
  }

  @Test
  public void getCommandLineArgs_variable_replace() throws IOException {
    Variables v = new Variables();
    SqoopConfig config = new SqoopConfig() {
    };
    String connect = "jdbc:mysql://localhost:3306/test";

    config.setConnect( "${testing}" );

    List<String> args = SqoopUtils.getCommandLineArgs( config, null );

    assertEquals( 2, args.size() );
    assertEquals( "--connect", args.get( 0 ) );
    assertEquals( "${testing}", args.get( 1 ) );

    v.setVariable( "testing", connect );
    args = SqoopUtils.getCommandLineArgs( config, v );

    assertEquals( 2, args.size() );
    assertEquals( "--connect", args.get( 0 ) );
    assertEquals( connect, args.get( 1 ) );
  }

  @Test
  public void getCommandLineArgs_variable_replace_flag() throws IOException {
    Variables v = new Variables();
    SqoopConfig config = new SqoopConfig() {
    };

    config.setVerbose( "${testing}" );
    assertEquals( 0, SqoopUtils.getCommandLineArgs( config, null ).size() );

    v.setVariable( "testing", Boolean.TRUE.toString() );
    List<String> args = SqoopUtils.getCommandLineArgs( config, v );
    assertEquals( 1, args.size() );
    assertEquals( "--verbose", args.get( 0 ) );
  }

  @Test
  public void getCommandLineArgs_command_line_string() throws IOException {
    SqoopConfig config = new SqoopConfig() {
    };

    config.setMode( JobEntryMode.ADVANCED_COMMAND_LINE.name() );

    config.setTable( "table-from-property" );

    config.setCommandLine( "--table \"\\\"table with whitespace\" --testing test --new-boolean-property" );

    // Make sure the command line arguments from the property "commandLine" are used and could represent currently
    // unknown values
    List<String> args = SqoopUtils.getCommandLineArgs( config, null );
    assertEquals( 5, args.size() );
    assertEquals( "--table", args.get( 0 ) );
    assertEquals( "\"table with whitespace", args.get( 1 ) );
    assertEquals( "--testing", args.get( 2 ) );
    assertEquals( "test", args.get( 3 ) );
    assertEquals( "--new-boolean-property", args.get( 4 ) );
  }

  @Test
  public void parseCommandLine() throws IOException {
    String s =
        "sqoop import --connect jdbc:mysql://db.foo.com/corp --table EMPLOYEES "
            + "--username ${username} -P --enclosed-by \"\\\"\" --fields-terminated-by \"\\t\"";

    List<String> args = SqoopUtils.parseCommandLine( s, null, false );
    assertEquals( 13, args.size() );

    assertEquals( "sqoop", args.get( 0 ) );
    assertEquals( "import", args.get( 1 ) );
    assertEquals( "--connect", args.get( 2 ) );
    assertEquals( "jdbc:mysql://db.foo.com/corp", args.get( 3 ) );
    assertEquals( "--table", args.get( 4 ) );
    assertEquals( "EMPLOYEES", args.get( 5 ) );
    assertEquals( "--username", args.get( 6 ) );
    assertEquals( "${username}", args.get( 7 ) );
    assertEquals( "-P", args.get( 8 ) );
    assertEquals( "--enclosed-by", args.get( 9 ) );
    assertEquals( "\"", args.get( 10 ) );
    assertEquals( "--fields-terminated-by", args.get( 11 ) );
    assertEquals( "\\t", args.get( 12 ) );
  }

  @Test
  public void parseCommandLine_variables() throws IOException {
    VariableSpace variableSpace = new Variables();
    variableSpace.setVariable( "username", "bob" );
    String s = "sqoop import --connect jdbc:mysql://db.foo.com/corp --table EMPLOYEES " + "--username ${username} -P";

    List<String> args = SqoopUtils.parseCommandLine( s, variableSpace, true );
    assertEquals( 7, args.size() );

    assertEquals( "--connect", args.get( 0 ) );
    assertEquals( "jdbc:mysql://db.foo.com/corp", args.get( 1 ) );
    assertEquals( "--table", args.get( 2 ) );
    assertEquals( "EMPLOYEES", args.get( 3 ) );
    assertEquals( "--username", args.get( 4 ) );
    assertEquals( "bob", args.get( 5 ) );
    assertEquals( "-P", args.get( 6 ) );
  }

  @Test
  public void parseCommandLine_import_with_delimiters() throws IOException {
    VariableSpace variableSpace = new Variables();
    String s =
        "sqoop import --connect jdbc:mysql://db.foo.com/corp --table EMPLOYEES "
            + "--fields-terminated-by '\\t' --lines-terminated-by '\\n' "
            + "--optionally-enclosed-by '\\\"${}' --driver com.microsoft.jdbc.sqlserver.SQLServerDriver";
    System.out.println( s );

    List<String> args = SqoopUtils.parseCommandLine( s, variableSpace, false );
    assertEquals( 14, args.size() );

    assertEquals( "sqoop", args.get( 0 ) );
    assertEquals( "import", args.get( 1 ) );
    assertEquals( "--connect", args.get( 2 ) );
    assertEquals( "jdbc:mysql://db.foo.com/corp", args.get( 3 ) );
    assertEquals( "--table", args.get( 4 ) );
    assertEquals( "EMPLOYEES", args.get( 5 ) );
    assertEquals( "--fields-terminated-by", args.get( 6 ) );
    assertEquals( "\\t", args.get( 7 ) );
    assertEquals( "--lines-terminated-by", args.get( 8 ) );
    assertEquals( "\\n", args.get( 9 ) );
    assertEquals( "--optionally-enclosed-by", args.get( 10 ) );
    assertEquals( "\"${}", args.get( 11 ) );
    assertEquals( "--driver", args.get( 12 ) );
    assertEquals( "com.microsoft.jdbc.sqlserver.SQLServerDriver", args.get( 13 ) );
  }

  @Test
  public void generateCommandLineString() {
    SqoopConfig config = new SqoopConfig() {
    };

    config.setTable( "testing" );
    config.setConnect( "jdbc:oracle:thin://bogus/testing" );
    config.setBinDir( "dir with space" );
    config.setOptionallyEnclosedBy( "\\t" );

    assertEquals(
        "--connect jdbc:oracle:thin://bogus/testing --optionally-enclosed-by \"\\t\" --bindir \"dir with space\" --table testing",
        SqoopUtils.generateCommandLineString( config, null ) );
  }

  @Test
  public void generateCommandLineString_ignoringEmptyStringParameter() {
    MockConfig cfg = new MockConfig();
    cfg.setTest( Boolean.TRUE.toString() );
    cfg.setStrictlyNotEmpty( "" );

    assertEquals( "--test", SqoopUtils.generateCommandLineString( cfg, null ) );
  }

  @Test
  public void generateCommandLineString_notIgnoringStringOfSpaces() {
    MockConfig cfg = new MockConfig();
    cfg.setTest( Boolean.TRUE.toString() );
    cfg.setStrictlyNotEmpty( "   " );

    assertEquals( "--test --strictlyNotEmpty \"   \"", SqoopUtils.generateCommandLineString( cfg, null ) );
  }

  @Test
  public void generateCommandLineString_password() {
    SqoopConfig config = new SqoopConfig() {
    };

    config.setPassword( "password!!!" );

    config.setTable( "testing" );
    config.setBinDir( "dir with space" );
    config.setOptionallyEnclosedBy( "\\t" );

    assertEquals(
        "--password password!!! --optionally-enclosed-by \"\\t\" --bindir \"dir with space\" --table testing",
        SqoopUtils.generateCommandLineString( config, null ) );

    config.setPassword( "${password}" );
  }

  @Test
  public void generateCommandLineString_variables() {
    VariableSpace variableSpace = new Variables();
    SqoopConfig config = new SqoopConfig() {
    };

    variableSpace.setVariable( "table", "testing" );
    variableSpace.setVariable( "encloseChar", "\"" );

    config.setTable( "${table}" );
    config.setEnclosedBy( "${encloseChar}" );

    assertEquals( "--enclosed-by \"\\\"\" --table testing", SqoopUtils
        .generateCommandLineString( config, variableSpace ) );
  }

  @Test
  public void escapeEscapeSequences() {
    assertEquals( "\\t", SqoopUtils.escapeEscapeSequences( "\t" ) );
    assertEquals( "\\b", SqoopUtils.escapeEscapeSequences( "\b" ) );
    assertEquals( "\\n", SqoopUtils.escapeEscapeSequences( "\n" ) );
    assertEquals( "\\r", SqoopUtils.escapeEscapeSequences( "\r" ) );
    assertEquals( "\\f", SqoopUtils.escapeEscapeSequences( "\f" ) );
  }

  @Test
  public void configureFromCommandLine() throws IOException, KettleException {
    SqoopConfig config = new SqoopConfig() {
    };
    PersistentPropertyChangeListener l = new PersistentPropertyChangeListener();

    assertNull( config.getTable() );
    config.setCommandLine( "--table test" );

    config.addPropertyChangeListener( l );

    SqoopUtils.configureFromCommandLine( config, config.getCommandLine(), null );

    assertEquals( "test", config.getTable() );

    List<PropertyChangeEvent> receivedEventsWithChanges = l.getReceivedEventsWithChanges();
    assertEquals( 1, receivedEventsWithChanges.size() );
    PropertyChangeEvent evt = receivedEventsWithChanges.get( 0 );
    assertEquals( "table", evt.getPropertyName() );
    assertNull( evt.getOldValue() );
    assertEquals( "test", evt.getNewValue() );
  }

  @Test
  public void configureFromCommandLine_no_shorthand_password() throws IOException {
    SqoopConfig config = new SqoopConfig() {
    };

    try {
      SqoopUtils.configureFromCommandLine( config, "-P", null );
      fail( "Expected KettleException for invalid argument" );
    } catch ( KettleException ex ) {
      assertEquals( "Unknown argument(s): -P", ex.getMessage().trim() );
    }
  }

  @Test
  public void configureFromCommandLine_roundtrip() throws IOException, KettleException {
    VariableSpace variableSpace = new Variables();
    SqoopConfig config = new SqoopConfig() {
    };
    SqoopConfig config2 = new SqoopConfig() {
    };
    SqoopConfig config3 = new SqoopConfig() {
    };

    variableSpace.setVariable( "table", "testing" );

    config.setTable( "${table}" );
    config.setConnect( "jdbc:mysql://localhost/bogus" );
    config.setEnclosedBy( "\"" );
    config.setVerbose( Boolean.TRUE.toString() );

    String s = SqoopUtils.generateCommandLineString( config, null );
    SqoopUtils.configureFromCommandLine( config2, s, null );

    assertEquals( config.getTable(), config2.getTable() );
    assertEquals( config.getConnect(), config2.getConnect() );
    assertEquals( config.getEnclosedBy(), config2.getEnclosedBy() );
    assertEquals( config.getVerbose(), config2.getVerbose() );

    SqoopUtils.configureFromCommandLine( config3, s, variableSpace );
    assertEquals( "testing", config3.getTable() );
    assertEquals( config.getConnect(), config3.getConnect() );
    assertEquals( "\"", config3.getEnclosedBy() );
    assertEquals( Boolean.TRUE.toString(), config3.getVerbose() );
  }

  @Test( expected = KettleException.class )
  public void configureFromCommandLine_with_an_empty_string() throws Exception {
    MockConfig cfg = new MockConfig();
    String cmd = "--strictlyNotEmpty    ";
    SqoopUtils.configureFromCommandLine( cfg, cmd, new Variables() );
  }

  @Test
  public void configureFromCommandLine_with_a_string_of_spaces() throws Exception {
    final String SPACES = "   ";
    String cmd = String.format( "--strictlyNotEmpty \"%s\"", SPACES );

    MockConfig cfg = new MockConfig();
    SqoopUtils.configureFromCommandLine( cfg, cmd, new Variables() );
    assertThat( cfg.getStrictlyNotEmpty(), is( equalTo( SPACES ) ) );
  }

  @Test
  public void findAllArguments() {
    MockConfig config = new MockConfig();

    Set<? extends ArgumentWrapper> args = SqoopUtils.findAllArguments( config );

    for ( ArgumentWrapper arg : args ) {
      if ( arg.getName().equals( "test" ) ) {
        assertEquals( "Test", arg.getDisplayName() );
        assertTrue( arg.isFlag() );
        return;
      }
    }
    fail( "Unable to find test @CommandLineArgument annotated field" );
  }

  @Test
  public void findMethod() {
    assertNotNull( SqoopUtils.findMethod( MockConfig.class, "Connect", null, "bogus", "get" ) );
    assertNotNull( SqoopUtils.findMethod( MockConfig.class, "Test", null, "bogus", "get" ) );
    assertNull( SqoopUtils.findMethod( MockConfig.class, "Test", null, "bogus" ) );
  }

  @Test
  public void parseCommandLine_numericArgs() throws Exception {
    // PDI-10554
    List<String> args = SqoopUtils.parseCommandLine( "--num-mappers 55", new Variables(), true );
    assertEquals( "there should be a couple of args", 2, args.size() );
    assertEquals( "the first arg does not match ", args.get( 0 ), "--num-mappers" );
    assertEquals( "the second arg does not match ", args.get( 1 ), "55" );
  }

}
