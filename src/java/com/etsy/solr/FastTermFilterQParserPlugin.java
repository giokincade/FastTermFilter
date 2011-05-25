package com.etsy.solr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrConstantScoreQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.etsy.util.Pair;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class FastTermFilterQParserPlugin extends QParserPlugin {
  public static final String NAME = "fastTermFilter";
  private static final Logger LOGGER = LoggerFactory.getLogger(FastTermFilterQParserPlugin.class);
  
  @Override
  public void init(NamedList args) {

  }
  
  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new FastTermFilterQParser(qstr, localParams, params, req);
    
  }
  
  public class FastTermFilterQParser extends QParser { 
    public FastTermFilterQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
      super(qstr,localParams, params, req);   
    }
    

    /****
     * The expected format is "cacheName:contextKeyName cacheName:contextKeyName" etc.
     */
    @Override
    public Query parse()
    {
      List<Pair<String, String>> cacheNameAndContextNames = Lists.transform(Arrays.asList(this.qstr.split(" ")), new Function <String, Pair<String,String>> () {
        @Override
        public Pair<String,String> apply(String x){
          String[] pair = x.split(":");
          if (pair.length < 2) {
            LOGGER.error(String.format("Could not parse cacheName+ContextKeyName pair from %s", x));
            return null;
          }          
          return Pair.create(pair[0], pair[1]); 
        }
      });
      
      List<Pair<BooleanClause.Occur, SolrConstantScoreQuery>> queries = Lists.transform(cacheNameAndContextNames, new Function<Pair<String,String>, Pair<BooleanClause.Occur, SolrConstantScoreQuery>>(){
        @Override
        public Pair<BooleanClause.Occur, SolrConstantScoreQuery> apply(Pair<String,String> cacheNameAndContextName) {
          String cacheName = cacheNameAndContextName.getA();
          BooleanClause.Occur occur = null; 
            
          switch(cacheName.charAt(0)) {
            case '+': 
              occur = BooleanClause.Occur.MUST;
              cacheName = cacheName.substring(1);
              break;
            case '-':
              occur = BooleanClause.Occur.MUST_NOT;
              cacheName = cacheName.substring(1);
              break;
            default: 
              occur = BooleanClause.Occur.SHOULD;
              break;
          }
          return Pair.create(occur, getQuery(cacheName, cacheNameAndContextName.getB()));
        }
      });
      
      BooleanQuery result = new BooleanQuery(true);
      
      for(Pair<BooleanClause.Occur, SolrConstantScoreQuery> query: queries) {
        result.add(query.getB(), query.getA());
      }
      
      return result;        
    }
    
    private SolrConstantScoreQuery getQuery(String cacheName, String contextKeyName) {
      int[] terms;
      
      if(contextKeyName.length() > 0
          && contextKeyName.charAt(0) == '[') {
        terms = getTermsFromQueryString(contextKeyName);
        
      }
      else {
        terms = (int[]) req.getContext().get(contextKeyName);
      }
       
      SolrCache solrCache = req.getSearcher().getCache(cacheName);
      
      if (!(solrCache instanceof  FillableIntTermCache))
        throw new IllegalArgumentException(String.format("Cache %s is not a FillableIntTermCache.", cacheName));
      
      FillableIntTermCache cache = (FillableIntTermCache)solrCache;
      DocumentNumberFilter filter = cache.getFilterForTerms(terms);
      
      return new SolrConstantScoreQuery(filter);
      
    }
    
    private int[] getTermsFromQueryString(String query) {
      if(query.length() > 2
          && query.startsWith("[") 
          && query.endsWith("]")) {
        
        Iterator split = Splitter.on(',').trimResults().omitEmptyStrings().split(query.substring(1, query.length()-1)).iterator();

        LinkedList<Integer> terms = new LinkedList<Integer>();
        while(split.hasNext()) {
          terms.add(Integer.parseInt((String)split.next()));
        }
        
        return ArrayUtils.toPrimitive(terms.toArray(new Integer[0]));        
      }
      return new int[0];
    }
    

  }
  
}
