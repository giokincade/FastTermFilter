package com.etsy.solr;

import it.unimi.dsi.fastutil.ints.AbstractInt2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntCollection;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import com.etsy.solr.LuceneDocumentNumber;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;


public class IntToLuceneDocumentNumberMultiMap implements Multimap<Integer, LuceneDocumentNumber> {
  private Map<String,AbstractInt2ObjectMap<IntCollection>> segmentNameToIntMap; 
  private int size = 0;
  
  public IntToLuceneDocumentNumberMultiMap() {
    this.segmentNameToIntMap = Maps.newHashMap();
  }

  @Override
  public Map<Integer, Collection<LuceneDocumentNumber>> asMap() {
    throw new UnsupportedOperationException(); 
  }

  @Override
  public void clear() {
    this.segmentNameToIntMap.clear();
    this.size = 0;
  }

  @Override
  public boolean containsEntry(Object key, Object value) {
    if (!(key instanceof Integer)
      || !(value instanceof LuceneDocumentNumber)) {
      throw new IllegalArgumentException();  
    }
    return containsEntry((Integer)key, (LuceneDocumentNumber)value);
  }
  

  public boolean containsEntry(Integer key, LuceneDocumentNumber value) {
    return containsEntry((int)key, value);
  }
  

  public boolean containsEntry(int key, LuceneDocumentNumber value) {
    if(this.containsKey(key)) {
      return this.get(key).contains(value);
    }
    return false;
  }
  
  @Override
  public boolean containsKey(Object key) {
    if (!(key instanceof Integer))
      throw new IllegalArgumentException();
    
    return this.containsKey((int)(Integer)key); 
  }
  
  public boolean containsKey(int key) {
    for(java.util.Map.Entry<String, AbstractInt2ObjectMap<IntCollection>> entry: this.segmentNameToIntMap.entrySet()) {
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

    throw new UnsupportedOperationException();   
  }
  
  public boolean containsValue(LuceneDocumentNumber value) {
    String segmentName = value.getSegmentName();
    if(this.segmentNameToIntMap.containsKey(segmentName)) {
      AbstractInt2ObjectMap<IntCollection> map = this.segmentNameToIntMap.get(segmentName);
      for(IntCollection docNumbers: map.values()) {
        if(docNumbers.contains(value.getDocumentNumber()))
          return true;
      }
    }
    return false;
  }

  @Override
  public Collection<Entry<Integer, LuceneDocumentNumber>> entries() {
    Collection<Entry<Integer, LuceneDocumentNumber>> set = new HashSet<Entry<Integer,LuceneDocumentNumber>>();
    
    for(Entry<String, AbstractInt2ObjectMap<IntCollection>> entry: this.segmentNameToIntMap.entrySet()) {      
      AbstractInt2ObjectMap<IntCollection> map = entry.getValue();
      String segmentName = entry.getKey();
      
      for(Entry<Integer,IntCollection> segmentEntry: map.entrySet()) {
        Integer i = segmentEntry.getKey();
        Collection<LuceneDocumentNumber> docs = LuceneDocumentNumber.fromListOfDocumentNumbers(segmentName, segmentEntry.getValue());
        
        for(LuceneDocumentNumber doc:docs) {
          AbstractMap.SimpleEntry<Integer, LuceneDocumentNumber> resultEntry = new AbstractMap.SimpleEntry<Integer, LuceneDocumentNumber>(i, doc);
          set.add(resultEntry);        
        }
      }            
    }
    
    return set;
  }

  @Override
  public Collection<LuceneDocumentNumber> get(Integer key) {
    if(!(key instanceof Integer))
      throw new IllegalArgumentException();
    
    return this.get((int)(Integer)key);
  }
  
  public Collection<LuceneDocumentNumber> get(int key){
    Collection<LuceneDocumentNumber> returnList = Lists.newArrayList();
    
    for(Entry<String, AbstractInt2ObjectMap<IntCollection>> entry: this.segmentNameToIntMap.entrySet()) {
      AbstractInt2ObjectMap<IntCollection> map = entry.getValue();
      String segmentName = entry.getKey();
      
      if (map.containsKey(key)) {
        returnList.addAll(LuceneDocumentNumber.fromListOfDocumentNumbers(segmentName, map.get(key)));
      }    
    }
    return returnList;
  }

  @Override
  public boolean isEmpty() {
    return this.segmentNameToIntMap.isEmpty();
  }

  @Override
  public Set<Integer> keySet() {
    Set<Integer> set = new HashSet<Integer>();
    
    for(AbstractInt2ObjectMap<IntCollection> map: this.segmentNameToIntMap.values()) {
      set.addAll(map.keySet());      
    }
    
    return set;    
  }

  @Override
  public Multiset<Integer> keys() {
    Multiset<Integer> set = HashMultiset.create();
    
    for(AbstractInt2ObjectMap<IntCollection> map: this.segmentNameToIntMap.values()) {
      for(Entry<Integer, IntCollection> entry: map.entrySet()) {
        IntCollection docs = entry.getValue();
        for(int i = 0; i < docs.size(); i++) {
          set.add(entry.getKey());          
        }
      }
    }
    
    return set;    
  }

  @Override

  public boolean put(Integer key, LuceneDocumentNumber value) {
    //Initializing segments doesn't have the same benefit for the multimap
    //because we can't know how many of the docs in the given segment actually have
    //the same term.
    String segmentName = value.getSegmentName();
    int luceneDocNumber = value.getDocumentNumber();
    
    if(!this.segmentNameToIntMap.containsKey(segmentName)) {
      this.segmentNameToIntMap.put(segmentName, this.newMap());
    }

    AbstractInt2ObjectMap<IntCollection> map = this.segmentNameToIntMap.get(segmentName);
    
    if(map.containsKey(key)) {
      map.get(key).add(luceneDocNumber);
    }
    else {
      IntCollection collection = this.newCollection();
      collection.add(luceneDocNumber);
      map.put(key, collection);
    }
    size++;
    return true;
  }
  
  protected IntCollection newCollection() {
    return new IntArrayList();
  }
  
  protected AbstractInt2ObjectMap<IntCollection> newMap() {
    return new Int2ObjectLinkedOpenHashMap<IntCollection>();
  }
  
  
  @Override
  public boolean putAll(Multimap<? extends Integer, ? extends LuceneDocumentNumber> vals) {
    boolean ret = false;
    for(Entry<? extends Integer, ? extends LuceneDocumentNumber> entry: vals.entries()) {
      ret = ret || this.put(entry.getKey(), entry.getValue());
    }
    return ret;
  }

  @Override
  public boolean putAll(Integer key, Iterable<? extends LuceneDocumentNumber> vals) {
    boolean ret = false;
    
    for(LuceneDocumentNumber doc: vals) {
      ret = ret || this.put(key, doc);
    }
    return ret;
  }

  @Override
  public boolean remove(Object arg0, Object arg1) {
    throw new UnsupportedOperationException();   
  }

  @Override
  public Collection<LuceneDocumentNumber> removeAll(Object arg0) {
    throw new UnsupportedOperationException();   
  }

  @Override
  public Collection<LuceneDocumentNumber> replaceValues(Integer arg0, Iterable<? extends LuceneDocumentNumber> arg1) {
    throw new UnsupportedOperationException();   
  }

  @Override
  public int size() {
    return this.size;
  }

  @Override
  public Collection<LuceneDocumentNumber> values() {
    Set<LuceneDocumentNumber> set = new HashSet<LuceneDocumentNumber>();
    
    for(Entry<String, AbstractInt2ObjectMap<IntCollection>> entry: this.segmentNameToIntMap.entrySet()) {
      Collection<LuceneDocumentNumber> segmentSet = new ArrayList<LuceneDocumentNumber>();
      AbstractInt2ObjectMap<IntCollection> map = entry.getValue();
      String segmentName = entry.getKey();
      
      for(IntCollection docNums: map.values()) {
        segmentSet.addAll(LuceneDocumentNumber.fromListOfDocumentNumbers(segmentName, docNums));
      }
      
      set.addAll(segmentSet);      
    }
    
    return set;   
  }

}
