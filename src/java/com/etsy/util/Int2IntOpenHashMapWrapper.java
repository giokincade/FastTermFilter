package com.etsy.util;


import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

public class Int2IntOpenHashMapWrapper extends Int2IntOpenHashMap implements Int2IntMapWrapper {
  
  public Int2IntOpenHashMapWrapper(int numberOfDocs) {
    super(numberOfDocs);
  }

}
