package org.pentaho.hbase.shim;

public interface HBaseBytesUtil {

  int getSizeOfFloat();

  int getSizeOfDouble();

  int getSizeOfInt();

  int getSizeOfLong();

  int getSizeOfShort();

  byte[] toBytes(String aString);

  byte[] toBytes(int anInt);

  byte[] toBytes(long aLong);

  byte[] toBytes(float aFloat);

  byte[] toBytes(double aDouble);

  byte[] toBytesBinary(String value);

  String toString(byte[] value);

  long toLong(byte[] value);

  int toInt(byte[] value);

  float toFloat(byte[] value);

  double toDouble(byte[] value);

  short toShort(byte[] value);
}
