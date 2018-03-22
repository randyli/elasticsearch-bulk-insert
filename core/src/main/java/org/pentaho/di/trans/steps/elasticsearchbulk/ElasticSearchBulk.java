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
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
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

  private RestHighLevelClient client = null;

  private String index;
  private String type;

  private int batchSize = 2;

  private boolean isJsonInsert = false;
  private int jsonFieldIdx = 0;

  private String idOutFieldName = null;
  private Integer idFieldIndex = null;

  private Long timeout = null;
  private TimeUnit timeoutUnit = TimeUnit.MILLISECONDS;

  // private long duration = 0L;
  private int numberOfErrors = 0;

  private boolean stopOnError = true;
  private boolean useOutput = true;

  private Map<String, String> columnsToJson;
  private boolean hasFields;

  private BulkRequest currentRequest;

  public ElasticSearchBulk( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
    currentRequest = new BulkRequest();
  }

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    Object[] rowData = getRow();
    if ( rowData == null ) {
      if ( currentRequest.numberOfActions() > 0 ) {
        // didn't fill a whole batch
        processBatch( false );
      }
      setOutputDone();
      return false;
    }

    if ( first ) {
      first = false;
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
    } else {
      idFieldIndex = null;
    }
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

  /**
   * @param rowMeta
   *          The metadata for the row to be indexed
   * @param row
   *          The data for the row to be indexed
   */

  private boolean indexRow( RowMetaInterface rowMeta, Object[] row ) throws KettleStepException {
    try {

      String id = null;
      if ( idFieldIndex == null ) {
        throw new KettleStepException("ID field must be set" );
      }
      id = "" + row[idFieldIndex]; // "" just in case field isn't string

      IndexRequest req = new IndexRequest(index, type, id);
      if ( isJsonInsert ) {
        addSourceFromJsonString( row, req );
      } else {
        addSourceFromRowFields( req, rowMeta, row );
      }
      currentRequest.add(req);




      if ( currentRequest.numberOfActions() >= batchSize ) {
        return processBatch( true );
      } else {
        return true;
      }

    } catch ( KettleStepException e ) {
      throw e;
    } catch ( Exception e ) {
      throw new KettleStepException( BaseMessages.getString( PKG, "ElasticSearchBulk.Log.Exception", e
          .getLocalizedMessage() ), e );
    }
  }

  /**
   * @param row
   * @param requestBuilder
   */
  private void addSourceFromJsonString( Object[] row, IndexRequest requestBuilder ) throws KettleStepException {
    Object jsonString = row[jsonFieldIdx];
    if ( jsonString instanceof byte[] ) {
      requestBuilder.source( (byte[]) jsonString,  XContentType.JSON);
    } else if ( jsonString instanceof String ) {
      requestBuilder.source((String) jsonString ,  XContentType.JSON);
    } else {
      throw new KettleStepException( BaseMessages.getString( "ElasticSearchBulk.Error.NoJsonFieldFormat" ) );
    }
  }

  /**
   * @param requestBuilder
   * @param rowMeta
   * @param row
   * @throws IOException
   */
  private void addSourceFromRowFields( IndexRequest requestBuilder, RowMetaInterface rowMeta, Object[] row ) {
    Map<String, Object> jsonMap = new HashMap<>();
    for ( int i = 0; i < rowMeta.size(); i++ ) {
      if ( idFieldIndex != null && i == idFieldIndex ) { // skip id
        continue;
      }

      ValueMetaInterface valueMeta = rowMeta.getValueMeta( i );
      String name = hasFields ? columnsToJson.get( valueMeta.getName() ) : valueMeta.getName();
      Object value = row[i];
      if ( value instanceof Date && value.getClass() != Date.class ) {
        Date subDate = (Date) value;
        // create a genuine Date object, or jsonBuilder will not recognize it
        value = new Date( subDate.getTime() );
      }
      if ( StringUtils.isNotBlank( name ) ) {
        jsonMap.put( name, value );
      }
    }
    requestBuilder.source( jsonMap );
  }

  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (ElasticSearchBulkMeta) smi;
    data = (ElasticSearchBulkData) sdi;

    if ( super.init( smi, sdi ) ) {
      try {
        numberOfErrors = 0;
        initFromMeta();
        return true;
      } catch ( Exception e ) {
        logError( BaseMessages.getString( PKG, "ElasticSearchBulk.Log.ErrorOccurredDuringStepInitialize" )
            + e.getMessage() );
      }
      return true;
    }
    return false;
  }

  private void initFromMeta() {
    index = environmentSubstitute( meta.getIndex() );
    type = environmentSubstitute( meta.getType() );
    batchSize = meta.getBatchSizeInt( this );
    try {
      timeout = Long.parseLong( environmentSubstitute( meta.getTimeOut() ) );
    } catch ( NumberFormatException e ) {
      timeout = null;
    }
    timeoutUnit = meta.getTimeoutUnit();
    isJsonInsert = meta.isJsonInsert();
    useOutput = meta.isUseOutput();
    stopOnError = meta.isStopOnError();

    columnsToJson = meta.getFieldsMap();
    this.hasFields = columnsToJson.size() > 0;


    List<ElasticSearchBulkMeta.Server> servers = meta.getServers();
    HttpHost[] hosts = new HttpHost[servers.size()];
    for ( int i=0; i<hosts.length; i++ ) {
      //showMessage(BaseMessages.getString( PKG, "ElasticSearchBulkDialog.Test.TestOKTitle" ));
      hosts[i] = new HttpHost(servers.get(i).getAddr().getHostName(), servers.get(i).getAddr().getPort());
    }
    client = new RestHighLevelClient(RestClient.builder(hosts));

  }

  private boolean processBatch( boolean makeNew ) {
    boolean responseOk = false;

    try {

      BulkResponse response = client.bulk(currentRequest);
      if(response.hasFailures()){
        logError( response.buildFailureMessage() );
      }else{
        responseOk = true;
      }
/*
      if ( timeout != null && timeoutUnit != null ) {
        response = actionFuture.actionGet( timeout, timeoutUnit );
      } else {
        response = actionFuture.actionGet();
      }
  */
    } catch (IOException e) {
      logError( e.getLocalizedMessage() );
    }



    if ( makeNew ) {
      currentRequest = new BulkRequest() ;
      data.nextBufferRowIdx = 0;
      data.inputRowBuffer = new Object[batchSize][];
    } else {
      currentRequest = null;
      data.inputRowBuffer = null;
    }
    return responseOk;
  }


  private void disposeClient() {

    if ( client != null ) {
      try {
        client.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
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
