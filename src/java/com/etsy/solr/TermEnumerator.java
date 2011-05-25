package com.etsy.solr;

import java.io.IOException;

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.search.SolrIndexReader;
import org.apache.solr.search.SolrIndexSearcher;

import com.etsy.util.Pair;
import com.etsy.util.Receiver;

public class TermEnumerator {

  public static void enumerate(SolrIndexSearcher searcher, String fieldName, Receiver<SegmentInfo> segmentReceiver,
      Receiver<Pair<String, LuceneDocumentNumber>> receiver) throws IOException {
    SolrIndexReader reader = (SolrIndexReader) searcher.getIndexReader();
    SolrIndexReader[] leafReaders = reader.getLeafReaders();

    for (SolrIndexReader leaf : leafReaders) {
      IndexReader segmentReader = leaf.getWrappedReader();

      if (!(segmentReader instanceof SegmentReader)) throw new UnsupportedOperationException("Leaf Readers must be an instance of SegmentReader.");

      String segmentName = ((SegmentReader) segmentReader).getSegmentName();
      segmentReceiver.receive(new SegmentInfo(segmentName, segmentReader.numDocs()));

      TermsEnum termsIterator = segmentReader.terms(fieldName).iterator();
      BytesRef term = termsIterator.next();
      DocsEnum docs = null;

      while (term != null) {
        String termString = term.utf8ToString();
        docs = termsIterator.docs(null, docs);
        int docId = docs.nextDoc();

        while (docId != DocIdSetIterator.NO_MORE_DOCS) {
          receiver.receive(Pair.create(termString, new LuceneDocumentNumber(segmentName, docId)));
          docId = docs.nextDoc();
        }
        term = termsIterator.next();
      }
    }
  }

  public static void enumerate(SolrIndexSearcher searcher, String fieldName, Receiver<Pair<String, LuceneDocumentNumber>> receiver) throws IOException {
    enumerate(searcher, fieldName, new Receiver<SegmentInfo>() {
      @Override
      public void receive(SegmentInfo segment) {
        // NO-OP
      }
    }, receiver);

  }

  public static class SegmentInfo {
    public final String segmentName;
    public final int numDocs;

    public SegmentInfo(String segmentName, int numDocs) {
      this.segmentName = segmentName;
      this.numDocs = numDocs;
    }
  }

}
