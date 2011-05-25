package com.etsy.solr;

import java.util.Collection;

import com.google.common.collect.Lists;

public class LuceneDocumentNumber implements Comparable<LuceneDocumentNumber>{
  private String segmentName;
  private int documentNumber;
  
  public LuceneDocumentNumber(String segmentName, int documentNumber) {
    this.segmentName = segmentName.intern();
    this.documentNumber = documentNumber;
  }
  
  public static Collection<LuceneDocumentNumber> fromListOfDocumentNumbers(String segmentName, Collection<Integer> docNumbers) {
    Collection<LuceneDocumentNumber> returnList = Lists.newArrayList();
    
    for(Integer docNumber:docNumbers) {
      returnList.add(new LuceneDocumentNumber(segmentName, docNumber));      
    }
    
    return returnList;
  }
  
  public String getSegmentName() {
    return this.segmentName;    
  }
  
  public int getDocumentNumber() {
    return this.documentNumber;
  }
  
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    LuceneDocumentNumber that = (LuceneDocumentNumber) o;
    if(this.segmentName != that.segmentName) return false; //remember teh segment names are interned.
    if(this.documentNumber != that.documentNumber) return false;
    
    return true;
  }
  
  @Override
  public int hashCode() {
    int result = this.documentNumber;
    return 31*result + this.segmentName.hashCode();    
  }
  
  @Override
  public String toString() {
    return String.format("LuceneDocumentNumber{%s, %d}", this.segmentName, this.documentNumber);
  }

  @Override
  public int compareTo(LuceneDocumentNumber o) {
    if (this.equals(o)) return 0;
    
    if (this.segmentName.compareTo(o.getSegmentName()) != 0) return  this.segmentName.compareTo(o.getSegmentName());
    
    return this.documentNumber - o.documentNumber;
  }
  

}
