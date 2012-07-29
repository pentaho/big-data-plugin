package org.pentaho.hbase.shim;

import org.apache.hadoop.hbase.util.Bytes;

public class DefaultHBaseBytesUtil implements HBaseBytesUtil {

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
}
