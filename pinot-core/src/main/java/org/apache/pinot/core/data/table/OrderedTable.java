package org.apache.pinot.core.data.table;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;


public class OrderedTable {
  private final int _capacity;
  private final int _youngGenCapacity;

  private Int2IntOpenHashMap _youngGenMap;
  private Int2IntOpenHashMap _oldGenMap;
  private int _kthSmallest;
  private int _min;
  private int _max;
  private int _ymin;
  private int _ymax;

  public OrderedTable(int capacity) {
    _capacity = capacity;
    _youngGenCapacity = _capacity / 4;
    _youngGenMap = new Int2IntOpenHashMap(_youngGenCapacity);
    _oldGenMap = new Int2IntOpenHashMap(_capacity);

    _kthSmallest = Integer.MIN_VALUE;

    _min = Integer.MAX_VALUE;
    _max = Integer.MIN_VALUE;

    _ymin = Integer.MAX_VALUE;
    _ymax = Integer.MIN_VALUE;
  }

  public void upsert(int key, int value) {
    int v = _youngGenMap.getOrDefault(key, 0);
    value += v;

    // Values go into working map first.
    _youngGenMap.put(key, value);
    _ymin = Math.min(value, _ymin);
    _ymax = Math.max(value, _ymax);

    // If working copy is full, then we need to see if any values can be moved to distilled map.
    if (_youngGenMap.size() > _youngGenCapacity) {
      if (_min == Integer.MAX_VALUE || _ymax > _min) {
        promote();
      }

      // Reset the working map.
      _youngGenMap = new Int2IntOpenHashMap(_youngGenCapacity);
    }
  }

  public void promote() {

    // If distilledMap size == capacity then compute 75% - do inside of the OrderedMap.
    // - Drop bottom 25% from the distilled map.
    // Iterate over working map, and if the key exists - update in distilled map.
    // - If the key does not exist, add to distilled map if the value is greater than 75%.

    Int2IntOpenHashMap map = new Int2IntOpenHashMap(_capacity);
    if (_oldGenMap.size() >= _capacity) {

      int[] array = new int[_oldGenMap.size()];
      int i = 0;
      _min = Integer.MAX_VALUE;
      _max = Integer.MIN_VALUE;

      // Find the 25% percentile of the distilled map.
      for (Int2IntMap.Entry entry : _oldGenMap.int2IntEntrySet()) {
        array[i++] = entry.getIntValue();
      }
      int k = array.length / 4;
      QuickSelect.kthSmallest(array, 0, array.length - 1, k);
      _kthSmallest = array[k];

      // Drop the bottom 25%
      for (Int2IntMap.Entry entry : _oldGenMap.int2IntEntrySet()) {
        int value = entry.getIntValue();
        if (value > _kthSmallest) {
          map.put(entry.getIntKey(), value);
          _min = Math.min(value, _min);
          _max = Math.max(value, _max);
        }
      }
    } else {
      map.putAll(_oldGenMap);
    }

    for (Int2IntMap.Entry entry : _youngGenMap.int2IntEntrySet()) {
      int key = entry.getIntKey();
      int value = entry.getIntValue();
      if (value >= _kthSmallest || map.containsKey(key)) {
        map.addTo(key, value);
        _min = Math.min(value, _min);
        _max = Math.max(value, _max);
      }
    }

    _oldGenMap = map;
  }

  public int get(int key) {
    return _youngGenMap.get(key);
  }

  public int finish() {
    PriorityQueue<KVPair> pq = new PriorityQueue<>();
    for (Int2IntMap.Entry entry : _oldGenMap.int2IntEntrySet()) {
      pq.add(new KVPair(entry.getIntKey(), entry.getIntValue()));
    }

    int v = 0;
    while (!pq.isEmpty()) {
      v = pq.poll()._value;
    }
    return v;
  }

  private static void benchmarkNew(int N, int M, int[] keys) {
    for (int i = 0; i < 2; i++) {
      long start = System.currentTimeMillis();
      OrderedTable map = new OrderedTable(M);
      for (int j = 0; j < N; j++) {
        map.upsert(keys[j], 1);
      }
      long end = System.currentTimeMillis();
      System.out.println("Top Element: " + map.finish() + " Time: " + (end - start));
    }
  }

  private static void benchmarkBase(int N, int M, int[] keys) {
    for (int j = 0; j < 10; j++) {

      long start = System.currentTimeMillis();
      Int2IntOpenHashMap map = new Int2IntOpenHashMap(M);
      for (int i = 0; i < N; i++) {
        int value = map.getOrDefault(keys[i], 0);
        value++;
        map.put(keys[i], value);
      }

      PriorityQueue<KVPair> pq = new PriorityQueue<>(Collections.reverseOrder());
      for (Int2IntMap.Entry entry : map.int2IntEntrySet()) {
        if (pq.size() < M) {
          pq.add(new KVPair(entry.getIntKey(), entry.getIntValue()));
        } else {
          KVPair top = pq.peek();
          if (top.getValue() < entry.getIntValue()) {
            pq.add(new KVPair(entry.getIntKey(), entry.getIntValue()));
            pq.poll();
          }
        }
      }

      long end = System.currentTimeMillis();
      System.out.println("Top " + pq.peek().getValue() + " Map size: " + map.size() + " Total time: " + (end - start));
    }
  }

  private static class KVPair implements Comparable<KVPair> {
    private int _key;
    private int _value;

    public KVPair(int key, int value) {
      _key = key;
      _value = value;
    }

    public int getKey() {
      return _key;
    }

    public int getValue() {
      return _value;
    }

    @Override
    public int compareTo(KVPair o) {
      return _value - o._value;
    }
  }

  public static void main(String[] args) {
    int N = 10_000_000;
    int M = 1_000_000;
    int K = 10_000;
    Random random = new Random(System.nanoTime());
    int[] keys = new int[N];

    for (int i = 0; i < N; i++) {
      int r = random.nextInt(M);
      keys[i] = r;
    }


//    benchmarkBase(N, M, keys);
    benchmarkNew(N, K, keys);

    Int2IntOpenHashMap map = new Int2IntOpenHashMap(M);
    for (int i = 0; i < keys.length; i++) {
      map.addTo(keys[i], 1);
    }

    List<Integer> list = new ArrayList<>();
    for (Int2IntMap.Entry entry : map.int2IntEntrySet()) {
      list.add(entry.getIntValue());
    }
    Collections.sort(list, (a, b) -> (b - a));

    System.out.println("Real max: " + list.get(0));
  }
}
