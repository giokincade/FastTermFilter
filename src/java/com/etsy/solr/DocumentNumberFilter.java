package com.etsy.solr;

import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntIterator;
import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.OpenBitSet;
import org.apache.solr.search.SolrIndexReader;

/*****
 * 
 * A Filter for a set of Lucene DocIds. 
 * 
 * @author Giovanni Fernandez-Kincade
 *
 */
@SuppressWarnings("serial")
public class DocumentNumberFilter extends Filter {
  private final Map<String, IntCollection> segmentToDocumentNumberCollectionMap;
  
  /***
   * 
   * @param segmentToDocumentNumberCollectionMap A map from Segment Name to the collection of Lucene DocIds 
   * that should be included in the filter.
   */
  public DocumentNumberFilter(Map<String, IntCollection> segmentToDocumentNumberCollectionMap) {
    this.segmentToDocumentNumberCollectionMap = segmentToDocumentNumberCollectionMap;    
  }

  @Override
  public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
    OpenBitSet bits = new OpenBitSet(reader.maxDoc());
    
    if (!(reader instanceof SolrIndexReader))
      throw new UnsupportedOperationException("The DocumentNumberFilter can only be called with a SolrIndexReader.");
    
    SolrIndexReader solrReader = (SolrIndexReader)reader;    
    IndexReader segmentReader = solrReader.getWrappedReader();
    
    if (!(segmentReader instanceof SegmentReader))
      throw new UnsupportedOperationException("The DocumentNumberFilter Filter can only be called with a SolrIndexReader that wraps a SegmentReader.");
    
    String segmentName = ((SegmentReader)segmentReader).getSegmentName();
    
    if (!this.segmentToDocumentNumberCollectionMap.containsKey(segmentName))
        return bits;
    
    IntIterator iterator = this.segmentToDocumentNumberCollectionMap.get(segmentName).iterator();
    
    while(iterator.hasNext()) {
      int docId = iterator.nextInt();
      if(docId <= reader.maxDoc()) {
        bits.set(docId);
      }            
    }
    
    return bits;
  }
}
