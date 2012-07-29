package org.pentaho.hbase.shim;

public interface HBaseBytesUtil {

  byte[] toBytes(String aString);

  byte[] toBytes(int anInt);

  byte[] toBytes(long aLong);

  byte[] toBytes(float aFloat);

  byte[] toBytes(double aDouble);

  byte[] toBytesBinary(String value);

}
