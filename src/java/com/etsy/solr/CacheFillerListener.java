package com.etsy.solr;

import java.util.List;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheFillerListener implements SolrEventListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(CacheFillerListener.class.getName());
  SolrCore core;
  List<String> cacheNames;
  
  public CacheFillerListener(SolrCore core) {
    this.core = core;
  }
  
  @Override
  public void init(NamedList args) {    
    this.cacheNames = (List<String>)args.get("caches");
  }

  @Override
  public void postCommit() {    
  }

  @Override
  public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {    
    for (String cacheName: this.cacheNames)
    {
      LOGGER.info(String.format("Filling %s", cacheName));
      SolrCache cache = newSearcher.getCache(cacheName);
      
      if(!(cache instanceof FillableCache))
        LOGGER.error(String.format("Could not fill %s cache because it's not an instance of FillableCache.", cacheName));
      else {
        try {
          long start = System.currentTimeMillis();
          ((FillableCache)cache).fill(newSearcher, currentSearcher);
          LOGGER.info(String.format("Filled %s. Size=%d. Time=%dmilliseconds.", cacheName,cache.size(),System.currentTimeMillis() -start));
        } catch (Exception e) {
          LOGGER.error(String.format("Could not fill %s cache", cacheName), e);
        }
      }
    }
  }

}
