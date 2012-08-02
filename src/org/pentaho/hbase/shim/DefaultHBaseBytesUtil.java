package org.pentaho.hbase.shim;

import org.apache.hadoop.hbase.util.Bytes;

public class DefaultHBaseBytesUtil implements HBaseBytesUtil {

  public int getSizeOfFloat() {
    return Bytes.SIZEOF_FLOAT;
  }

  public int getSizeOfDouble() {
    return Bytes.SIZEOF_DOUBLE;
  }

  public int getSizeOfInt() {
    return Bytes.SIZEOF_INT;
  }

  public int getSizeOfLong() {
    return Bytes.SIZEOF_LONG;
  }

  public int getSizeOfShort() {
    return Bytes.SIZEOF_SHORT;
  }

  public byte[] toBytes(String aString) {
    return Bytes.toBytes(aString);
  }

  public byte[] toBytes(int anInt) {
    return Bytes.toBytes(anInt);
  }

  public byte[] toBytes(long aLong) {
    return Bytes.toBytes(aLong);
  }

  public byte[] toBytes(float aFloat) {
    return Bytes.toBytes(aFloat);
  }

  public byte[] toBytes(double aDouble) {
    return Bytes.toBytes(aDouble);
  }

  public byte[] toBytesBinary(String value) {
    return Bytes.toBytesBinary(value);
  }

  public String toString(byte[] value) {
    return Bytes.toString(value);
  }

  public long toLong(byte[] value) {
    return Bytes.toLong(value);
  }

  public int toInt(byte[] value) {
    return Bytes.toInt(value);
  }

  public float toFloat(byte[] value) {
    return Bytes.toFloat(value);
  }

  public double toDouble(byte[] value) {
    return Bytes.toDouble(value);
  }

  public short toShort(byte[] value) {
    return Bytes.toShort(value);
  }
}
