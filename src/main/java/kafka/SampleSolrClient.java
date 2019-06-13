package kafka;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.MapSolrParams;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SampleSolrClient {
    private static SolrClient createSolrClient(String url) {
        return new HttpSolrClient.Builder(url)
                .withConnectionTimeout(10000)
                .withSocketTimeout(60000)
                .build();
    }

    public static void main(String[] args) throws IOException, SolrServerException {
        if (args.length < 2 ) {
            throw new IllegalArgumentException(String.format("Missing arguments, try with solr url and index"));
        }
        String solrUrl = args[0];
        String index = args[1];
        SolrClient solrClient = createSolrClient(solrUrl);
        final Map<String, String> queryParamMap = new HashMap<String, String>();
        queryParamMap.put("q", "*:*");
        //queryParamMap.put("fl", "id, name");
        MapSolrParams queryParams = new MapSolrParams(queryParamMap);

        final QueryResponse response = solrClient.query(index, queryParams);
        final SolrDocumentList documents = response.getResults();

        for(SolrDocument document : documents) {
            System.out.println("document:" + document);
        }
    }
}
