package com.speedb.api;

import java.io.File;
import java.util.Map;


/**
 * Main interface to read data from a SpeedB store.
 * <p>
 * <code>SpeedB.createReader()</code> method and then call the
 * <code>get()</code> method to fetch. Call the
 * <code>close()</code> to liberate resources when done.
 */
public interface StoreReader<K, V> {

  public V get(K key);

  /**
   * Closes the store reader and free resources.
   * <p>
   * A closed reader can't be reopened.
   */
  public void close();
}
