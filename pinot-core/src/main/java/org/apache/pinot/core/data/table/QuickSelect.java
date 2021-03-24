package org.apache.pinot.core.data.table;

import java.util.Arrays;
import java.util.Random;


public class QuickSelect {
  // Implementation of QuickSelect
  public static int kthSmallest(int a[], int left, int right, int k) {
    while (left <= right) {

      // Partition a[left..right] around a pivot
      // and find the position of the pivot
      int pivotIndex = partition(a, left, right);

      // If pivot itself is the k-th smallest element
      if (pivotIndex == k - 1) {
        return a[pivotIndex];
      }

      // If there are more than k-1 elements on
      // left of pivot, then k-th smallest must be
      // on left side.
      else if (pivotIndex > k - 1) {
        right = pivotIndex - 1;
      }

      // Else k-th smallest is on right side.
      else {
        left = pivotIndex + 1;
      }
    }
    return -1;
  }

  // Standard Lomuto partition function
  private static int partition(int arr[], int low, int high) {
    int temp;
    int pivot = arr[high];
    int i = (low - 1);
    for (int j = low; j <= high - 1; j++) {
      if (arr[j] <= pivot) {
        i++;
        temp = arr[i];
        arr[i] = arr[j];
        arr[j] = temp;
      }
    }

    temp = arr[i + 1];
    arr[i + 1] = arr[high];
    arr[high] = temp;

    return (i + 1);
  }

  public static void main(String[] args) {
    int N = 10_000;
    int M = 10000;
    int[] array = new int[N];

    int max = Integer.MIN_VALUE;
    Random random = new Random(System.nanoTime());
    for (int i = 0; i < N; i++) {
      array[i] = random.nextInt(M);
      max = Math.max(max, array[i]);
    }

    int[] copy = Arrays.copyOf(array, array.length);


    int k = 999;
    int val = QuickSelect.kthSmallest(array, 0, array.length - 1, k);
    System.out.println("K: " + k + " Kth Smallest: " + val);
    System.out.println("Max: " + max);
    System.out.println("Sorted k:" + copy[k]);

    long start = System.currentTimeMillis();
//    Arrays.sort(array);
    long end = System.currentTimeMillis();
    System.out.println("Sorting time: " + (end - start));
//    if (true) return;

    for (int i = 0; i < 10000; i++) {
      int[] a = Arrays.copyOf(array, array.length);
      start = System.currentTimeMillis();
      QuickSelect.kthSmallest(a, 0, array.length - 1, 2500);
      end = System.currentTimeMillis();
      System.out.println("Total time: " + (end - start));
    }

    if (true) return;

    for (int l = 1; l <= M; l++) {
      int v = QuickSelect.kthSmallest(array, 0, array.length - 1, l);
      if (v != copy[l]) {
        System.out.println("Failed");
      }
    }
    System.out.println("PASSED");
  }
}
