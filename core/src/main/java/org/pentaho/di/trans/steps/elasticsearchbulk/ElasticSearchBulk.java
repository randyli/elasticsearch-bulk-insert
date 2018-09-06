/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.elasticsearchbulk;

import java.io.IOException;
import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.json.simple.JSONObject;

/**
 * Does bulk insert of data into ElasticSearch
 *
 * @author webdetails
 * @since 16-02-2011
 */
public class ElasticSearchBulk extends BaseStep implements StepInterface {

  private static final String INSERT_ERROR_CODE = null;
  private static Class<?> PKG = ElasticSearchBulkMeta.class; // for i18n

  private ElasticSearchBulkMeta meta;
  private ElasticSearchBulkData data;

  private RestClient restClient = null;

  private String index;
  private String type;

  private int batchSize = 2;

  private boolean isJsonInsert = false;
  private int jsonFieldIdx = 0;

  private String idOutFieldName = null;
  private Integer idFieldIndex = null;

  // private long duration = 0L;
  private int numberOfErrors = 0;

  private boolean stopOnError = true;
  private boolean useOutput = true;

  private Map<String, String> columnsToJson;
  private boolean hasFields;

  private Vector<String> bulkData;

  public ElasticSearchBulk( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );

  }

  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (ElasticSearchBulkMeta) smi;
    data = (ElasticSearchBulkData) sdi;
    if ( super.init( smi, sdi ) ) {
      try {
        numberOfErrors = 0;
        initFromMeta();
        initClient();
        this.bulkData = new Vector<>();
      } catch ( Exception e ) {
        logError( BaseMessages.getString( PKG, "ElasticSearchBulk.Log.ErrorOccurredDuringStepInitialize" )
                + e.getLocalizedMessage() );
        e.printStackTrace();
        return false;
      }
      return true;
    }
    return false;
  }



  /**
   * Initialize <code>this.data</code>
   *
   * @throws KettleStepException
   */
  private void setupData() throws KettleStepException {
    data.nextBufferRowIdx = 0;
    data.inputRowMeta = getInputRowMeta().clone(); // only available after first getRow();
    data.inputRowBuffer = new Object[batchSize][];
    data.outputRowMeta = data.inputRowMeta.clone();
    meta.getFields( data.outputRowMeta, getStepname(), null, null, this, repository, metaStore );
  }

  private void initFieldIndexes() throws KettleStepException {
    if ( isJsonInsert ) {
      Integer idx = getFieldIdx( data.inputRowMeta, environmentSubstitute( meta.getJsonField() ) );
      if ( idx != null ) {
        jsonFieldIdx = idx.intValue();
      } else {
        throw new KettleStepException( BaseMessages.getString( PKG, "ElasticSearchBulk.Error.NoJsonField" ) );
      }
    }

    idOutFieldName = environmentSubstitute( meta.getIdOutField() );

    if ( StringUtils.isNotBlank( meta.getIdInField() ) ) {
      idFieldIndex = getFieldIdx( data.inputRowMeta, environmentSubstitute( meta.getIdInField() ) );
      if ( idFieldIndex == null ) {
        throw new KettleStepException( BaseMessages.getString( PKG, "ElasticSearchBulk.Error.InvalidIdField" ) );
      }
    }
  }

  private void initFromMeta() {
    index = environmentSubstitute( meta.getIndex() );
    type = environmentSubstitute( meta.getType() );
    batchSize = meta.getBatchSizeInt( this );

    isJsonInsert = meta.isJsonInsert();
    useOutput = meta.isUseOutput();
    stopOnError = meta.isStopOnError();

    columnsToJson = meta.getFieldsMap();
    this.hasFields = columnsToJson.size() > 0;
  }

  private void initClient() {
    List<ElasticSearchBulkMeta.Server> servers = meta.getServers();
    HttpHost[] hosts = new HttpHost[servers.size()];
    for ( int i=0; i<hosts.length; i++ ) {
      hosts[i] = new HttpHost(servers.get(i).address, servers.get(i).port);
    }

    // http basic authentication

    System.out.println("user name:"+meta.getUserName());
    System.out.println("password:"+meta.getPassword());
    System.out.println("connection timeout:"+meta.getConnectTimeout());
    System.out.println("socket timeout:"+meta.getSocketTimeout());
    System.out.println("max retry timeout:"+meta.getMaxRetryTimeout());
    RestClientBuilder builder = RestClient.builder(hosts);

    if(meta.getUserName()!= null && meta.getUserName() != "") {
      final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(meta.getUserName(), meta.getPassword()));
      builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
        @Override
        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
          return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        }
      });
    }
    // socket timeout connection timeout and max retry timeout
    RestClientBuilder.RequestConfigCallback requestConfigCallback = new RestClientBuilder.RequestConfigCallback() {
      @Override
      public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
        return requestConfigBuilder.setConnectTimeout(meta.getConnectTimeout()).setSocketTimeout(meta.getSocketTimeout());
      }
    };
    builder.setRequestConfigCallback(requestConfigCallback);
    builder.setMaxRetryTimeoutMillis(meta.getMaxRetryTimeout());
    restClient = builder.build();
  }

  private static Integer getFieldIdx( RowMetaInterface rowMeta, String fieldName ) {
    if ( fieldName == null ) {
      return null;
    }
    for ( int i = 0; i < rowMeta.size(); i++ ) {
      String name = rowMeta.getValueMeta( i ).getName();
      if ( fieldName.equals( name ) ) {
        return i;
      }
    }
    return null;
  }

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    Object[] rowData = getRow();
    if ( rowData == null ) {
      if ( bulkData.size() > 0 ) {
        // didn't fill a whole batch
        try {
          processBatch(false);
        }catch(IOException e){
          throw new KettleStepException("bulk insert error", e);
        }
      }
      setOutputDone();
      return false;
    }
    if(this.first) {
      this.first = false;
      setupData();
      initFieldIndexes();
    }
    try {
      data.inputRowBuffer[data.nextBufferRowIdx++] = rowData;
      return indexRow( data.inputRowMeta, rowData ) || !stopOnError;
    } catch ( KettleStepException e ) {
      throw e;
    } catch ( Exception e ) {
      String msg = BaseMessages.getString( PKG, "ElasticSearchBulk.Log.Exception", e.getLocalizedMessage() );
      logError( msg );
      throw new KettleStepException( msg, e );
    }
  }

  /**
   * @param rowMeta
   *          The metadata for the row to be indexed
   * @param row
   *          The data for the row to be indexed
   */

  private boolean indexRow( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
    try{
      if ( idFieldIndex == null ) {
        throw new KettleStepException("ID field must be set" );
      }
      String id = "" + row[idFieldIndex]; // "" just in case field isn't string

      Object jsonString;//row[jsonFieldIdx];
      if ( isJsonInsert ) {
        jsonString =  row[jsonFieldIdx];
      } else {
        jsonString = jsonEncodeRowFields(rowMeta, row);
      }
      addSourceFromJsonString( jsonString, id);
      if ( bulkData.size()/2 >= batchSize ) {
        return processBatch( true );
      }
      return true;
    } catch ( KettleStepException e ) {
      throw e;
    } catch ( Exception e ) {
      throw new KettleStepException( BaseMessages.getString( PKG, "ElasticSearchBulk.Log.Exception", e
          .getLocalizedMessage() ), e );
    }
  }

  private void addSourceFromJsonString( Object jsonString, String id ) throws KettleStepException {
    JSONObject obj = new JSONObject();
    obj.put("_index", meta.getIndex());
    obj.put("_type", meta.getType());
    obj.put("_id", id);
    JSONObject op = new JSONObject();
    op.put("index", obj);
    bulkData.add(op.toJSONString());
    if ( jsonString instanceof byte[] ) {
      bulkData.add(new String((byte[])jsonString));
    } else if ( jsonString instanceof String ) {
      bulkData.add((String)jsonString);
    } else {
      throw new KettleStepException( BaseMessages.getString( "ElasticSearchBulk.Error.NoJsonFieldFormat" ) );
    }
  }

  /**
   * @param rowMeta
   * @param row
   * @throws IOException
   */
  private String jsonEncodeRowFields( RowMetaInterface rowMeta, Object[] row ) {
    JSONObject obj = new JSONObject();
    for ( int i = 0; i < rowMeta.size(); i++ ) {
      ValueMetaInterface valueMeta = rowMeta.getValueMeta( i );
      String name = hasFields ? columnsToJson.get( valueMeta.getName() ) : valueMeta.getName();
      Object value = row[i];
      if ( value instanceof Date) {
        Date subDate = (Date) value;
        // create a genuine Date object, or jsonBuilder will not recognize it
        value = subDate.toString();
      }
      if ( StringUtils.isNotBlank( name ) ) {
        obj.put( name, value );
      }
    }
    return obj.toJSONString();
  }

  private boolean processBatch( boolean makeNew ) throws IOException {
    StringBuilder s = new StringBuilder();

    for(int i=0; i< bulkData.size(); i++){
      s.append(bulkData.get(i));
      s.append("\n");
    }
    bulkData.clear();
    //System.out.println(s);
    NStringEntity entity = new NStringEntity(s.toString());
    entity.setContentType("application/x-ndjson");
    //Request r = new Request("POST", "/_bulk", Collections.emptyMap(), entity);
    Response response = restClient.performRequest("POST","/_bulk", Collections.emptyMap(), entity);
    //System.out.println(response);
    String responseBody = EntityUtils.toString(response.getEntity());
    JSONParser parser=new JSONParser();
    try {
      JSONObject res = (JSONObject) parser.parse(responseBody);
      Boolean isError = (Boolean)res.get("errors");
      if(isError == false){
        return true;
      }
      JSONArray items = (JSONArray)res.get("items");
      for(int i=0; i< items.size(); i++){
        JSONObject item = (JSONObject) items.get(i);
        if(!"201".equals(item.get("status"))){
          logError("Some error on bulk:"+ item.toJSONString());
          if(stopOnError) {
            return false;
          }
        }
      }
    }catch(ParseException e){
      logError(e.getMessage());
      return false;
    }
    return true;
  }


  private void disposeClient() throws IOException{
    if(restClient!=null){
      restClient.close();
      restClient = null;
    }
  }

  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (ElasticSearchBulkMeta) smi;
    data = (ElasticSearchBulkData) sdi;
    try {
      disposeClient();
    } catch ( Exception e ) {
      logError( e.getLocalizedMessage(), e );
    }
    super.dispose( smi, sdi );
  }
}
