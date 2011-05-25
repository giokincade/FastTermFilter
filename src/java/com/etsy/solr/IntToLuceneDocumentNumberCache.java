package com.etsy.solr;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntCollection;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.CacheRegenerator;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.etsy.solr.TermEnumerator.SegmentInfo;
import com.etsy.util.Pair;
import com.etsy.util.Receiver;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class IntToLuceneDocumentNumberCache implements SolrCache<Integer, Integer>, FillableIntTermCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(IntToLuceneDocumentNumberCache.class.getName());
  private String fieldName;
  private State state;
  private String name;
  private IntToLuceneDocumentNumberMap map;

  @Override
  public Object init(Map args, Object persistence, CacheRegenerator regenerator) {
    state = State.CREATED;
    fieldName = (String) args.get("fieldName");
    name = (String) args.get("name");
    return null;
  }

  @Override
  public String getName() {
    return IntToLuceneDocumentNumberCache.class.getName();
  }

  @Override
  public String getVersion() {
    return "1";
  }

  @Override
  public String getDescription() {
    return "Cache of arbitrary field values (ints) to lucene document number";
  }

  @Override
  public Category getCategory() {
    return Category.CACHE;
  }

  @Override
  public String getSourceId() {
    return "SourceId";
  }

  @Override
  public String getSource() {
    return "Source";
  }

  @Override
  public URL[] getDocs() {
    return null;
  }

  @Override
  public NamedList getStatistics() {
    return null;
  }

  @Override
  public String name() {
    return this.name;
  }

  @Override
  public int size() {
    return this.map.size();
  }

  @Override
  public Integer put(Integer key, Integer value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Integer get(Integer key) {
    throw new UnsupportedOperationException();
  }

  public LuceneDocumentNumber get(int key) {
    return this.map.get(key);
  }

  public boolean contains(int key) {
    return this.map.containsKey(key);
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setState(org.apache.solr.search.SolrCache.State state) {
    this.state = state;
  }

  @Override
  public org.apache.solr.search.SolrCache.State getState() {
    return this.state;
  }

  @Override
  public void warm(SolrIndexSearcher searcher, SolrCache<Integer, Integer> old) throws IOException {

  }

  public void fill(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) throws CorruptIndexException, IOException {
    this.map = new IntToLuceneDocumentNumberMap(0.01);
    
    TermEnumerator.enumerate(newSearcher,
        this.fieldName,
        new Receiver<SegmentInfo>() {
          @Override
          public void receive(SegmentInfo s) {
            IntToLuceneDocumentNumberCache.this.map.initializeNewSegment(s.segmentName, s.numDocs);
          }
        },
        new Receiver<Pair<String,LuceneDocumentNumber>>() {
          @Override
          public void receive(Pair<String,LuceneDocumentNumber> termAndDoc) {
            IntToLuceneDocumentNumberCache.this.map.put((int)Integer.parseInt(termAndDoc.getA()), termAndDoc.getB());
          }
        });
    
  }
  
  public DocumentNumberFilter getFilterForTerms(int[] terms) {
    Map<String, IntCollection> segmentToDocIdCollectionMap = this.getDocIds(terms);
    return new DocumentNumberFilter(segmentToDocIdCollectionMap);    
  }
  
  private Map<String, IntCollection> getDocIds(int[] terms){
    
    Map<String, IntCollection> docIdMap = Maps.newHashMap();
    List<Integer> cacheMisses = Lists.newLinkedList();      

    long translationStart = System.currentTimeMillis();
    for(int term: terms) {
      if(this.map.containsKey(term)) {
        LuceneDocumentNumber doc = this.map.get(term); 
        if(docIdMap.containsKey(doc.getSegmentName())) {
          docIdMap.get(doc.getSegmentName()).add(doc.getDocumentNumber());
        }
        else {
          IntCollection docIds = new IntArrayList();
          docIds.add(doc.getDocumentNumber());
          docIdMap.put(doc.getSegmentName(), docIds);
        }
      }
      else {
        cacheMisses.add(term);
      }        
    }
    
    long translationEnd = System.currentTimeMillis();
    
    LOGGER.info(String.format("TermCount=%d TranslationTimeMS=%d", terms.length, translationEnd-translationStart));
    
    if (cacheMisses.size() > 0)
      LOGGER.info("CacheMisses = " + cacheMisses.toString());
    
    return docIdMap;
  }
  

  @Override
  public void close() {}

}
