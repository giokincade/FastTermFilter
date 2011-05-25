package com.etsy.solr;

import it.unimi.dsi.fastutil.ints.Int2IntFunction;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.etsy.util.Int2IntOpenHashMapWrapper;
import com.etsy.util.Int2IntMapWrapper;

public class IntToLuceneDocumentNumberMap implements Map<Integer,LuceneDocumentNumber>{
  private static final Logger LOGGER = LoggerFactory.getLogger(IntToLuceneDocumentNumberMap.class.getName());
  private Map<String, Int2IntMapWrapper> segmentNameToIntMap;
  private int size;
  private double falsePositiveProbability;
 
  
  public IntToLuceneDocumentNumberMap(double falsePositiveProbability) {
    this.segmentNameToIntMap = new HashMap<String, Int2IntMapWrapper>();
    this.size = 0;
    this.falsePositiveProbability = falsePositiveProbability;
  }
  
  @Override
  public int size() {
    return this.size;
  }

  @Override
  public boolean isEmpty() {
    return this.size < 1;
  }
  
  @Override
  public boolean containsKey(Object key) {
    if (!(key instanceof Integer))
      throw new IllegalArgumentException();
    
    return this.containsKey((int)(Integer)key); 
  }

  public boolean containsKey(int key) {
    for(java.util.Map.Entry<String, Int2IntMapWrapper> entry: this.segmentNameToIntMap.entrySet()) {
      if (entry.getValue().containsKey(key)) {
        return true;
      }    
    }
    return false;
  }

  @Override
  public boolean containsValue(Object value) {
    if (!(value instanceof LuceneDocumentNumber))
      throw new IllegalArgumentException();
    
    return this.containsValue((LuceneDocumentNumber)value);
  }
  
  public boolean containsValue(LuceneDocumentNumber value) {
    String segmentName = value.getSegmentName();
    if(this.segmentNameToIntMap.containsKey(segmentName))
        return this.segmentNameToIntMap.get(segmentName).containsValue(value.getDocumentNumber());
    
    return false;
  }
  
  public void initializeNewSegment(String segmentName, int numberOfDocs) {
    if(!this.segmentNameToIntMap.containsKey(segmentName)) {
      
      LOGGER.info(String.format("Creating BloomInt2IntMap for %s. NumberOfDocs=%d"
                  , segmentName, numberOfDocs));
      
      Int2IntMapWrapper map = getNewInt2IntMap(numberOfDocs);
      this.segmentNameToIntMap.put(segmentName, map);
      return;
    }
    else {
      throw new UnsupportedOperationException();      
    }
  }
  
  protected Int2IntMapWrapper getNewInt2IntMap(int numberOfDocs) {
    return new Int2IntOpenHashMapWrapper(numberOfDocs);
  }

  @Override
  public LuceneDocumentNumber get(Object key) {
    if(!(key instanceof Integer))
      throw new IllegalArgumentException();
    
    return this.get((int)(Integer)key);
  }
  
  public LuceneDocumentNumber get(int key) {
    for(Entry<String, Int2IntMapWrapper> entry: this.segmentNameToIntMap.entrySet()) {
      Int2IntMapWrapper map = entry.getValue();
      
      if (map.containsKey(key)) {
        return new LuceneDocumentNumber(entry.getKey(), map.get(key));
      }    
    }
    return null;
  }

  @Override
  public LuceneDocumentNumber put(Integer key, LuceneDocumentNumber value) {
    String segmentName = value.getSegmentName();
    int luceneDocNumber = value.getDocumentNumber();
    
    if(this.segmentNameToIntMap.containsKey(segmentName)) {
      int oldValue = this.segmentNameToIntMap.get(segmentName).put((int)key, (int)luceneDocNumber);
      size++;
      return new LuceneDocumentNumber(segmentName, oldValue);
    }
    else {
      throw new UnsupportedOperationException("New segments must be initialized before adding docs for them");      
    }    
  }

  @Override
  public LuceneDocumentNumber remove(Object key) {
    throw new UnsupportedOperationException();      
  }

  @Override
  public void putAll(Map<? extends Integer, ? extends LuceneDocumentNumber> m) {
    for(Entry<? extends Integer, ? extends LuceneDocumentNumber> entry: m.entrySet()) {
      this.put(entry.getKey(), entry.getValue());
    }    
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();    
  }

  @Override
  public Set<Integer> keySet() {
    Set<Integer> set = new HashSet<Integer>();
    
    for(Int2IntMapWrapper map: this.segmentNameToIntMap.values()) {
      set.addAll(map.keySet());      
    }
    
    return set;    
  }

  @Override
  public Collection<LuceneDocumentNumber> values() {
    Set<LuceneDocumentNumber> set = new HashSet<LuceneDocumentNumber>();
    
    for(Entry<String, Int2IntMapWrapper> entry: this.segmentNameToIntMap.entrySet()) {
      Collection<LuceneDocumentNumber> segmentSet = new ArrayList<LuceneDocumentNumber>();
      Int2IntMapWrapper map = entry.getValue();
      String segmentName = entry.getKey();
      
      for(Integer docNum: map.values()) {
        segmentSet.add(new LuceneDocumentNumber(segmentName, docNum));        
      }
      
      set.addAll(segmentSet);      
    }
    
    return set;
  }

  @Override
  public Set<java.util.Map.Entry<Integer, LuceneDocumentNumber>> entrySet() {
    Set<Entry<Integer, LuceneDocumentNumber>> set = new HashSet<Entry<Integer,LuceneDocumentNumber>>();
    
    for(Entry<String, Int2IntMapWrapper> entry: this.segmentNameToIntMap.entrySet()) {      
      Int2IntMapWrapper map = entry.getValue();
      String segmentName = entry.getKey();
      
      for(Entry<Integer,Integer> bloomEntry: map.entrySet()) {
        Integer i = bloomEntry.getKey();
        LuceneDocumentNumber doc = new LuceneDocumentNumber(segmentName, bloomEntry.getValue());
        
        AbstractMap.SimpleEntry<Integer, LuceneDocumentNumber> resultEntry = new AbstractMap.SimpleEntry<Integer, LuceneDocumentNumber>(i, doc);
        
        set.add(resultEntry);        
      }            
    }
    
    return set;
  }
}
