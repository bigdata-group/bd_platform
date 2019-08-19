package com.bigdata.api.hive.thift;

import org.apache.hive.service.cli.thrift.*;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;

import java.util.List;

/**
 * Created by jiwenlong on 2017/5/4.
 */
public class HiveServer2Thift {

    public static void main(String[] args) throws Exception
    {
        TSocket transport = new TSocket("udh-cluster-1",10000);
        transport.setTimeout(999999999);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        TCLIService.Client client = new TCLIService.Client(protocol);

        transport.open();

        TOpenSessionReq openReq = new TOpenSessionReq();
        TOpenSessionResp openResp = client.OpenSession(openReq);
        TSessionHandle sessHandle = openResp.getSessionHandle();
        TExecuteStatementReq execReq = new
                TExecuteStatementReq(sessHandle, "SELECT * FROM tab1");
        TExecuteStatementResp execResp =
                client.ExecuteStatement(execReq);
        TOperationHandle stmtHandle = execResp.getOperationHandle();
        TFetchResultsReq fetchReq = new TFetchResultsReq(stmtHandle,
                TFetchOrientation.FETCH_FIRST, 1);
        TFetchResultsResp resultsResp = client.FetchResults(fetchReq);

        TRowSet resultsSet = resultsResp.getResults();
        List<TRow> resultRows = resultsSet.getRows();
        for(TRow resultRow : resultRows){
            resultRow.toString();
        }

        TCloseOperationReq closeReq = new TCloseOperationReq();
        closeReq.setOperationHandle(stmtHandle);
        client.CloseOperation(closeReq);
        TCloseSessionReq closeConnectionReq = new
                TCloseSessionReq(sessHandle);
        client.CloseSession(closeConnectionReq);

        transport.close();

    }
}
