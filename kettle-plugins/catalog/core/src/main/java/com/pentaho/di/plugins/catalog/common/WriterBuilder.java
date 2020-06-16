package com.pentaho.di.plugins.catalog.common;

import com.pentaho.di.plugins.catalog.write.WritePayloadMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;

public class WriterBuilder {
  // for i18n purposes, needed by Translator2!!   $NON-NLS-1$
  private static final Class<?> writerBuilderClass = WritePayloadMeta.class;

  private static WriterBuilder inst = null;

  public enum TYPE {
   CSV, PARQUET
  };

  public static WriterBuilder instance() {
    if ( inst == null ) {
      inst = new WriterBuilder();
    }
    return inst;
  }

  private WriterBuilder() {

  }

  public WriterInterface createWriter( TYPE type ) throws KettleException {
    WriterInterface wi = null;
    switch ( type ) {
      case CSV:
        wi = new WriterCSVImpl();
        break;
      case PARQUET:
        wi = new WriterParquetImpl();
        break;
      default:
        throw new KettleException(  BaseMessages.getString( writerBuilderClass , "WriterBuilder.UnsupportedType" ) + ":" + type.toString() );
    }
    return wi;
  }

  public WriterInterface createWriter( String typeStr ) throws KettleException  {
    TYPE type = null;
    try {
      type = TYPE.valueOf( typeStr );
    } catch ( IllegalArgumentException | NullPointerException ex ) {
      throw new KettleException( BaseMessages.getString( writerBuilderClass , "WriterBuilder.InvalidType" ) + ":" + typeStr, ex );
    }
    return createWriter( type );
  }
}
