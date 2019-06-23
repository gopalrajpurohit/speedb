package com.speedb.api;

/**
 * Main interface to write data to a SpeedB store.
 * <p>
 * Users of this class should initialize it by using the
 * <code>SpeedB.createWriter()</code> method and then call the
 * <code>put()</code> method to insert. Call the
 * <code>close()</code> to liberate resources when done.
 * <p>
 * Note that duplicates aren't allowed.
 */
public interface StoreWriter<K, V> {

  /**
   * Close the store writer and append the data to the final destination. A
   * closed writer can't be reopened.
   */
  public void close();

  /**
   * Return the writer configuration. Configuration values should always be
   * set before calling the
   * <code>open()</code> method.
   *
   * @return the store configuration
   */
  public Configuration getConfiguration();

  /**
   * Put serialized key-value entry to the store. <p> Use only this method if
   * you've already serialized the key and the value in their SpeedB format.
   *
   * @param key a serialized key as a byte array
   * @param value a serialized value as a byte array
   * @throws NullPointerException if <code>key</code> or <code>value</code> is
   * null
   */
  public void put(K key, V value);
}
