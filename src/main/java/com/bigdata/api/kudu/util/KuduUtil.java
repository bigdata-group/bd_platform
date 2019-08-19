package com.bigdata.api.kudu.util;


import com.bigdata.common.PropertiesReader;
import org.apache.kudu.client.KuduClient;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KuduUtil {
    public static String kuduAddress = null;
    public static KuduClient client = null;
    public static Map<String, KuduClient> clientMap = null;
    static{
        Properties props = PropertiesReader.getBigDataConf();
        kuduAddress = props.getProperty("kudu-master-address");
        clientMap = new HashMap<String, KuduClient>();
    }

    public static KuduClient getDefaultClient(){
        if (client == null){
            synchronized (KuduUtil.class){
                if (client == null){
                    client =  new KuduClient.KuduClientBuilder(kuduAddress).build();
                }
            }
        }
        return client;
    }

    public static KuduClient getClient(String address){
        if (clientMap.get(address) == null){
            synchronized (KuduUtil.class){
                if (clientMap.get(address)  == null){
                    KuduClient kuduClient =  new KuduClient.KuduClientBuilder(address).build();
                    clientMap.put(address, kuduClient);
                }
            }
        }
        return clientMap.get(address);
    }


}