package com.etsy.solr;

import java.io.IOException;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.solr.search.SolrIndexSearcher;

public interface FillableCache {
  public void fill(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) throws CorruptIndexException, IOException;
}
