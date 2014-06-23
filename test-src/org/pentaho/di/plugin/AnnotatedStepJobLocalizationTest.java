package org.pentaho.di.plugin;

import static org.junit.Assert.*;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.amazon.s3.S3FileOutputMeta;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.plugins.JobEntryPluginType;
import org.pentaho.di.core.plugins.PluginInterface;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.PluginTypeInterface;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.i18n.LanguageChoice;
import org.scannotation.AnnotationDB;

public class AnnotatedStepJobLocalizationTest {
  private static PluginTypeInterface stepPluginType, jobPluginType;
  private static PluginRegistry reg;

  private static Set<String> steps;
  private static Set<String> jobs;

  @BeforeClass
  public static void setup() throws Exception {
    URL location = S3FileOutputMeta.class.getClassLoader().getResource( "org/pentaho/amazon/s3/S3FileOutputMeta.class" );

    File f = new File( location.toURI() );

    URI uri = f.getParentFile().getParentFile().getParentFile().toURI();
    URL root1 = uri.toURL();

    System.out.println( "Root classes URL:" + root1 );

    AnnotationDB adb = new AnnotationDB();
    adb.setScanClassAnnotations( true );
    adb.setScanMethodAnnotations( true );
    adb.setScanFieldAnnotations( true );
    adb.setScanParameterAnnotations( true );
    adb.scanArchives( root1 );

    Map<String, Set<String>> index = adb.getAnnotationIndex();

    steps = index.get( Step.class.getName() );
    jobs = index.get( JobEntry.class.getName() );

    LanguageChoice.getInstance().setDefaultLocale( Locale.US );

    reg = PluginRegistry.getInstance();
    reg.registerPluginType( StepPluginType.class );
    stepPluginType = reg.getPluginType( StepPluginType.class );
    jobPluginType = reg.getPluginType( JobEntryPluginType.class );
  }

  @Test
  public void test() throws Exception {
    for ( String stp : steps ) {
      testStep( stp );
    }

    System.out.println( "Steps tested:" + steps.size() );

    for ( String job : jobs ) {
      testJobEntry( job );
    }

    System.out.println( "Job Entries tested:" + jobs.size() );
  }

  private void testStep( String stp ) throws Exception {
    System.out.println( "Testing Annotated Step Localization for:" + stp );
    Class<?> clazz = Class.forName( stp );

    Step annot = clazz.getAnnotation( Step.class );
    assertNotNull( "The step has to be annotated:" + stp, annot );

    String id = annot.id();
    String nameKey = annot.name();
    String descKey = annot.description();
    String catKey = annot.categoryDescription();
    String pkg = annot.i18nPackageName();

    assertNotNull( "Id is mandatory:" + stp, id );
    assertNotNull( "Name key has to be specified:" + stp, nameKey );
    assertNotNull( "Description key has to be specified:" + stp, descKey );
    assertNotNull( "Category key has to be specified:" + stp, catKey );

    assertFalse( "Package has to be specified to be localizable:" + stp, Const.isEmpty( pkg ) );

    stepPluginType.handlePluginAnnotation( clazz, annot, null, false, null );
    PluginInterface plugin = reg.getPlugin( StepPluginType.class, id );

    String name = plugin.getName();
    String desc = plugin.getDescription();
    String cat = plugin.getCategory();

    assertNotNull( "Name should not be null:" + stp, name );
    assertNotNull( "Description should not be null:" + stp, desc );
    assertNotNull( "Category should not be null:" + stp, cat );

    assertFalse( "Name Translation should not be enclosed in '!'_'!':" + stp, name.startsWith( "!" )
        && name.endsWith( "!" ) );
    assertFalse( "Decription Translation should not be enclosed in '!'_'!':" + stp, desc.startsWith( "!" )
        && desc.endsWith( "!" ) );
    assertFalse( "Category Translation should not be enclosed in '!'_'!':" + stp, cat.startsWith( "!" )
        && cat.endsWith( "!" ) );
  }

  private void testJobEntry( String job ) throws Exception {
    System.out.println( "Testing Annotated Job Entry Localization for:" + job );
    Class<?> clazz = Class.forName( job );

    JobEntry annot = clazz.getAnnotation( JobEntry.class );
    assertNotNull( "The job entry has to be annotated:" + job, annot );

    String id = annot.id();
    String nameKey = annot.name();
    String descKey = annot.description();
    String catKey = annot.categoryDescription();
    String pkg = annot.i18nPackageName();

    assertNotNull( "Id is mandatory:" + job, id );
    assertNotNull( "Name key has to be specified:" + job, nameKey );
    assertNotNull( "Description key has to be specified:" + job, descKey );
    assertNotNull( "Category key has to be specified:" + job, catKey );

    assertFalse( "Package has to be specified to be localizable:" + job, Const.isEmpty( pkg ) );

    jobPluginType.handlePluginAnnotation( clazz, annot, null, false, null );
    PluginInterface plugin = reg.getPlugin( JobEntryPluginType.class, id );

    String name = plugin.getName();
    String desc = plugin.getDescription();
    String cat = plugin.getCategory();

    assertNotNull( "Name should not be null:" + job, name );
    assertNotNull( "Description should not be null:" + job, desc );
    assertNotNull( "Category should not be null:" + job, cat );

    assertFalse( "Name Translation should not be enclosed in '!'_'!':" + job, name.startsWith( "!" )
        && name.endsWith( "!" ) );
    assertFalse( "Decription Translation should not be enclosed in '!'_'!':" + job, desc.startsWith( "!" )
        && desc.endsWith( "!" ) );
    assertFalse( "Category Translation should not be enclosed in '!'_'!':" + job, cat.startsWith( "!" )
        && cat.endsWith( "!" ) );
  }
}
