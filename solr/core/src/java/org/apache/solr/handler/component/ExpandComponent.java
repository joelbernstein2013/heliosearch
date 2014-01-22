/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler.component;

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.search.CollapsingQParserPlugin;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;

import com.carrotsearch.hppc.IntObjectOpenHashMap;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Iterator;
import java.util.Map;

    /**
  * The ExpandComponent is designed to work with the CollapsingPostFilter.
  * The CollapsingPostFilter collapses a result set on a field.
  *
  * The ExpandComponent expands the groups for a single page.
  *
  *
  * http parameters:
  *
  * expand=true
  * expand.field=<field>
  * expand.limit=5
  * expand.sort=<field> asc|desc,
  *
  **/
    
public class ExpandComponent extends SearchComponent implements PluginInfoInitialized, SolrCoreAware {
  public static final String COMPONENT_NAME = "expand";
  private PluginInfo info = PluginInfo.EMPTY_INFO;

      @Override
  public void init(PluginInfo info) {
      this.info = info;
  }

      @Override
  public void prepare(ResponseBuilder rb) throws IOException {

  }

      @Override
  public void inform(SolrCore core) {

  }

        @Override
  public void process(ResponseBuilder rb) throws IOException {
    SolrQueryRequest req = rb.req;
    SolrParams params = req.getParams();
    if(params.get("expand") == null) {
        return;
    }

    boolean isShard = params.getBool(ShardParams.IS_SHARD, false);
    String ids = params.get(ShardParams.IDS);

    if(ids == null && isShard) {
        return;
    }

    String field = params.get("expand.field");
    String sortParam = params.get("expand.sort");
    String limitString = params.get("expand.limit");

    Sort sort = null;
    int limit = 5;

    if(limitString != null) {
      limit = Integer.parseInt(limitString);
    }

    if(sortParam != null) {
      sort = QueryParsing.parseSort(sortParam, rb.req);
    }

    SolrIndexSearcher searcher = req.getSearcher();
    AtomicReader reader = searcher.getAtomicReader();
    SortedDocValues values = FieldCache.DEFAULT.getTermsIndex(reader, field);
    FixedBitSet groupBits = new FixedBitSet(values.getValueCount());
    FixedBitSet collapsedSet = new FixedBitSet(reader.maxDoc());

    if(ids != null) {
    List<String> idArr = StrUtils.splitSmart(ids, ",", true);
    for(int i=0; i<idArr.size(); i++) {
        int id = Integer.parseInt(idArr.get(i));
        int ordValue = values.getOrd(id);
        collapsedSet.set(id);
        groupBits.set(ordValue);
      }
    } else {
      DocList docList = rb.getResults().docList;
      DocIterator it = docList.iterator();
      while(it.hasNext()) {
        Integer doc = it.next();
        int ordValue = values.getOrd(doc.intValue());
        collapsedSet.set(doc.intValue());
        groupBits.set(ordValue);
      }
    }

    Query query = rb.getQuery();
    List<Query> filters = rb.getFilters();
    List<Query> newFilters = new ArrayList();
    for(int i=0; i<filters.size(); i++) {
    Query q = filters.get(i);
      if(!(q instanceof CollapsingQParserPlugin.CollapsingPostFilter)) {
        newFilters.add(q);
      }
    }

    Collector collector = null;
    GroupExpandCollector groupExpandCollector = new GroupExpandCollector(values,groupBits, collapsedSet, limit, sort);
    SolrIndexSearcher.ProcessedFilter pfilter = searcher.getProcessedFilter(null, newFilters);
    if(pfilter.postFilter != null) {
      pfilter.postFilter.setLastDelegate(groupExpandCollector);
      collector = pfilter.postFilter;
    } else {
      collector = groupExpandCollector;
    }

    searcher.search(query, pfilter.filter, collector);
    IntObjectOpenHashMap groups = groupExpandCollector.getGroups();
    Iterator<IntObjectCursor> it = groups.iterator();
    HashMap<Integer, ScoreDoc[]> ordMap = new HashMap<Integer, ScoreDoc[]>();
    while(it.hasNext()) {
      IntObjectCursor cursor = it.next();
      TopDocsCollector topDocsCollector = (TopDocsCollector)cursor.value;
      int ord = cursor.key;
      TopDocs topDocs = topDocsCollector.topDocs();
      ScoreDoc[] scoreDocs = topDocs.scoreDocs;
      if(scoreDocs.length > 0) {
        ordMap.put(ord, scoreDocs);
      }
    }

    NamedList outList = new NamedList();
    Iterator entries = ordMap.entrySet().iterator();
    BytesRef bytesRef = new BytesRef();
    while(entries.hasNext()) {
      float maxScore = 0.0f;
      Map.Entry entry = (Map.Entry)entries.next();
      Integer ord = (Integer) entry.getKey();
      ScoreDoc[] scoreDocs = (ScoreDoc[])entry.getValue();
      int[] docs = new int[scoreDocs.length];
      float[] scores = new float[scoreDocs.length];
      for(int i=0; i<docs.length; i++) {
          ScoreDoc scoreDoc = scoreDocs[i];
          docs[i] = scoreDoc.doc;
          scores[i] = scoreDoc.score;
          if(scoreDoc.score > maxScore) {
            maxScore = scoreDoc.score;
          }
        }

      DocSlice slice = new DocSlice(0, docs.length, docs, scores, docs.length, maxScore);
      values.lookupOrd(ord.intValue(), bytesRef);
      String group = bytesRef.utf8ToString();
      outList.add(group, slice);
    }

    rb.rsp.add("expanded", outList);
  }
  
        @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {

  }
  
        @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    NamedList expanded = new NamedList();
    if ((sreq.purpose & ShardRequest.PURPOSE_GET_FIELDS) != 0) {
    for (ShardResponse srsp : sreq.responses) {
        NamedList response = srsp.getSolrResponse().getResponse();
        NamedList ex = (NamedList)response.get("expanded");
        for(int i=0; i<ex.size(); i++) {
            String name = ex.getName(i);
            SolrDocumentList val = (SolrDocumentList)ex.getVal(i);
            expanded.add(name, val);
          }
      }
      rb.req.getContext().put("expanded", expanded);
    }
  }
  
        @Override
  public void finishStage(ResponseBuilder rb) {
    if (rb.stage != ResponseBuilder.STAGE_GET_FIELDS) {
      return;
    }

    rb.rsp.add("expanded", rb.req.getContext().get("expanded"));
  }

  public class GroupExpandCollector extends Collector {
    private SortedDocValues docValues;
    private IntObjectOpenHashMap groups;
    private int docBase;
    private FixedBitSet groupBits;
    private FixedBitSet collapsedSet;

    public GroupExpandCollector(SortedDocValues docValues, FixedBitSet groupBits, FixedBitSet collapsedSet, int limit, Sort sort) throws IOException {
      groups = new IntObjectOpenHashMap();
      DocIdSetIterator iterator = groupBits.iterator();
      int group = -1;
      while((group = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        Collector collector = (sort == null) ? TopScoreDocCollector.create(limit, true) : TopFieldCollector.create(sort,limit, false, false,false, true);
        groups.put(group, collector);
      }

      this.collapsedSet = collapsedSet;
      this.groupBits = groupBits;
      this.docValues = docValues;
    }

    public IntObjectOpenHashMap getGroups() {
      return this.groups;
    }

    public boolean acceptsDocsOutOfOrder() {
      return false;
    }

    public void collect(int docId) throws IOException {
      int doc = docId+docBase;
      int ord = docValues.getOrd(doc);
      if(ord > -1 && groupBits.get(ord) && !collapsedSet.get(doc)) {
        Collector c = (Collector)groups.get(ord);
        c.collect(docId);
      }
    }

    public void setNextReader(AtomicReaderContext context) throws IOException {
      this.docBase = context.docBase;
      Iterator<IntObjectCursor> cursor = groups.iterator();
      while(cursor.hasNext()) {
        IntObjectCursor c = cursor.next();
        Collector collector = (Collector)c.value;
        collector.setNextReader(context);
      }
    }

    public void setScorer(Scorer scorer) throws IOException {
      Iterator<IntObjectCursor> cursor = groups.iterator();
      while(cursor.hasNext()) {
        IntObjectCursor c = cursor.next();
        Collector collector = (Collector)c.value;
        collector.setScorer(scorer);
      }
    }
  }

  ////////////////////////////////////////////
  ///  SolrInfoMBean
  ////////////////////////////////////////////

    @Override
  public String getDescription() {
    return "Expanding";
  }

    @Override
  public String getSource() {
    return null;
  }

    @Override
  public URL[] getDocs() {
    return null;
  }
}