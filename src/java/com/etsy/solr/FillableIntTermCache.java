package com.etsy.solr;

import java.io.IOException;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.solr.search.SolrIndexSearcher;

public interface FillableIntTermCache extends FillableCache{
  public DocumentNumberFilter getFilterForTerms(int[] terms);
}
