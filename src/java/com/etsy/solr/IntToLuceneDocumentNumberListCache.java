package com.etsy.solr;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntCollection;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.CacheRegenerator;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.etsy.util.Pair;
import com.etsy.util.Receiver;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class IntToLuceneDocumentNumberListCache implements SolrCache<Integer, Collection<LuceneDocumentNumber>>, FillableIntTermCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(IntToLuceneDocumentNumberListCache.class);
  private IntToLuceneDocumentNumberMultiMap map;
  private String fieldName;
  private State state;
  private String name;  

  @Override
  public void fill(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) throws CorruptIndexException, IOException {
    this.map = new IntToLuceneDocumentNumberMultiMap();
    TermEnumerator.enumerate(newSearcher,
        this.fieldName,
        new Receiver<Pair<String,LuceneDocumentNumber>>() {
          @Override
          public void receive(Pair<String,LuceneDocumentNumber> termAndDoc) {
            IntToLuceneDocumentNumberListCache.this.map.put((int)Integer.parseInt(termAndDoc.getA()), termAndDoc.getB());
          }
        });
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
    return "Cache of arbitrary terms (ints) to a list of lucene document numbers";  
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
  public Object init(Map args, Object persistence, CacheRegenerator regenerator) {
    state = State.CREATED;
    fieldName = (String) args.get("fieldName");
    name = (String) args.get("name");
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
  public Collection<LuceneDocumentNumber> put(Integer key, Collection<LuceneDocumentNumber> value) {
    throw new UnsupportedOperationException();  
  }

  @Override
  public Collection<LuceneDocumentNumber> get(Integer key) {
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
  public void warm(SolrIndexSearcher searcher, SolrCache<Integer, Collection<LuceneDocumentNumber>> old) throws IOException {
    
  }

  @Override
  public void close() {
    
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
        Collection<LuceneDocumentNumber> docs = this.map.get(term); 
        for(LuceneDocumentNumber doc: docs) {            
          if(docIdMap.containsKey(doc.getSegmentName())) {
            docIdMap.get(doc.getSegmentName()).add(doc.getDocumentNumber());
          }
          else {
            IntCollection docIds = new IntArrayList();
            docIds.add(doc.getDocumentNumber());
            docIdMap.put(doc.getSegmentName(), docIds);
          }
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

}
