package com.bigdata.api.hbase.wrapper;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.List;

/**
 * Created by jiwenlong on 2017/5/1.
 */
public class HBaseTableWrapper {
    private final Table tab;
    private BufferedMutator mutator = null;
    private boolean autoFlush = true;
    private final Connection conn;

    public HBaseTableWrapper(Connection conn, String tableName ) throws IOException {
        this.conn = conn;
        tab = conn.getTable(TableName.valueOf( tableName ) );
    }

    private synchronized BufferedMutator getBufferedMutator() throws IOException {
        if ( conn != null ) {
            if ( mutator == null ) {
                mutator = conn.getBufferedMutator( tab.getName() );
            }
        } else {
            throw new IOException( "Can't mutate the table " + tab.getName() );
        }

        return mutator;
    }

    public void setWriteBufferSize( long bufferSize ) throws IOException {
        tab.setWriteBufferSize( bufferSize );
    }

    public void setAutoFlush( boolean autoFlush ) throws IOException {
        this.autoFlush = autoFlush;
    }



    public boolean isAutoFlush() throws IOException {
        return autoFlush;
    }

    
    public ResultScanner getScanner(Scan s ) throws IOException {
        return tab.getScanner( s );
    }

    public Result get(Get toGet ) throws IOException {
        return tab.get( toGet );
    }

    public void flushCommits() throws IOException {
        getBufferedMutator().flush();
    }

    public void delete( Delete toDel ) throws IOException {
        getBufferedMutator().mutate( toDel );
        if ( autoFlush ) {
            getBufferedMutator().flush();
        }
    }

    public void delete( List<Delete> toDelLst ) throws IOException {
        getBufferedMutator().mutate( toDelLst );
        if ( autoFlush ) {
            getBufferedMutator().flush();
        }
    }


    public void close() throws IOException {
        tab.close();
        if ( mutator != null ) {
            mutator.close();
        }
    }

    public void put( HBasePutWrapper putWrapper ) throws IOException {
        if ( putWrapper == null ) {
            throw new NullPointerException( "NULL Put passed" );
        }
        put( putWrapper.getPut() );
    }

    private void put( Put toPut ) throws IOException {
        getBufferedMutator().mutate( toPut );
        if ( autoFlush ) {
            getBufferedMutator().flush();
        }
    }
}
