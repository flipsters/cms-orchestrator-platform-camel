package org.apache.camel.cms.orchestrator.utils;

import org.apache.camel.cms.orchestrator.aggregator.Payload;

import java.io.*;

/**
 * Created by pawas.kumar on 17/01/17.
 */
public class ByteUtils {

  public static byte[] getBytes(Object obj) throws IOException {
    if (obj == null)
      return null;
    byte[] bytes = null;
    ByteArrayOutputStream bos = null;
    ObjectOutputStream oos = null;
    try {
      bos = new ByteArrayOutputStream();
      oos = new ObjectOutputStream(bos);
      oos.writeObject(obj);
      oos.flush();
      bytes = bos.toByteArray();
    } finally {
      if (oos != null) {
        oos.close();
      }
      if (bos != null) {
        bos.close();
      }
    }
    return bytes;
  }

  public static <T> T fromBytes(byte[] bytes, Class<T> clazz)
          throws IOException, ClassNotFoundException {
    if (bytes == null)
      return null;
    T obj = null;
    ByteArrayInputStream bis = null;
    ObjectInputStream ois = null;
    try {
      bis = new ByteArrayInputStream(bytes);
      ois = new ObjectInputStream(bis);
      obj = (T) ois.readObject();
    } finally {
      if (bis != null) {
        bis.close();
      }
      if (ois != null) {
        ois.close();
      }
    }
    return obj;
  }
}
