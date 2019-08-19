package com.bigdata.api.hadoop.hdfs;

import java.io.IOException;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.bigdata.common.PropertiesReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * HDFS API
 */
public class HDFSHelper {
    private Configuration conf;
    private FileSystem hdfs;
    private HDFSHelper helper;

    /**
     * 单例，读取配置文件中的hdfs地址，获取默认的helper
     * @return
     * @throws IOException
     */
    public HDFSHelper getDefaultHDFSHealper() throws  IOException{
        if (helper == null){
            synchronized (HDFSHelper.class){
                if (helper == null){
                    conf = new Configuration();
                    Properties props = PropertiesReader.getBigDataConf();
                    conf.set("fs.defaultFS", props.getProperty("fs.defaultFS"));
                    this.hdfs = FileSystem.get(conf);
                }
            }
        }
        return helper;
    }

    public HDFSHelper(Configuration conf) throws IOException {
        this.conf = conf;
        this.hdfs = FileSystem.get(conf);
    }

    public HDFSHelper(Map<String, String> confMap) throws IOException {
        conf = new Configuration();
        for (Map.Entry<String, String> confEntry : confMap.entrySet()) {
            conf.set(confEntry.getKey(), confEntry.getValue());
        }
        this.hdfs = FileSystem.get(conf);
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    /**
     * 创建文件夹
     * @param dir
     * @throws IOException
     */
    public void createDir(String dir) throws IOException {
        Path path = new Path(dir);
        hdfs.mkdirs(path);
    }

    /**
     * 创建文件并写入内容
     * @param userName
     * @param file
     * @param fileContent
     * @throws Exception
     */
    public void createFile(String userName, final String file, final String fileContent) throws Exception {
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(userName);
        ugi.doAs(new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
                Path path = new Path(file);
                try (FSDataOutputStream outputStream = hdfs.create(path);) {
                    outputStream.write(fileContent.getBytes());
                    outputStream.flush();
                }
                return null;
            }
        });
    }

    /**
     * 上传服务器上的本地文件到HDFS
     * @param srcFile
     * @param dstFile
     * @throws IOException
     */
    public void putFile(String srcFile, String dstFile) throws IOException {
        this.putFile(false, true, srcFile, dstFile);
    }

    /**
     * 上传服务器上的本地文件到HDFS
     * @param delSrc
     * @param overwrite
     * @param srcFile
     * @param dstFile
     * @throws IOException
     */
    public void putFile(boolean delSrc, boolean overwrite, String srcFile, String dstFile) throws IOException {
        Path srcPath = new Path(srcFile);
        Path dstPath = new Path(dstFile);
        hdfs.copyFromLocalFile(delSrc, overwrite, srcPath, dstPath);
    }

    /**
     * HDFS上的文件重命名
     * @param srcName
     * @param dstName
     * @return
     * @throws IOException
     */
    public boolean renameFile(String srcName, String dstName) throws IOException {
        Path fromPath = new Path(srcName);
        Path toPath = new Path(dstName);
        boolean isRenamed = hdfs.rename(fromPath, toPath);
        return isRenamed;
    }

    /**
     * HDFS上的文件get到本地 相当于get命令或copytolocal
     * @param srcFile
     * @param dstFile
     * @throws IOException
     */
    public void getFile(String srcFile, String dstFile) throws IOException {
        this.getFile(false, srcFile, dstFile);
    }

    /**
     * HDFS上的文件get到本地 相当于get命令或copytolocal
     * @param delSrc
     * @param srcFile
     * @param dstFile
     * @throws IOException
     */
    public void getFile(boolean delSrc, String srcFile, String dstFile) throws IOException {
        Path srcPath = new Path(srcFile);
        Path dstPath = new Path(dstFile);
        hdfs.copyToLocalFile(delSrc, srcPath, dstPath);
    }

    /**
     * 读取hdfs上的文件到输出流
     * @param hdfsFile
     * @param outputStream
     * @throws IOException
     */
    public void readFile(String hdfsFile, OutputStream outputStream) throws IOException {
        try (FSDataInputStream fis = hdfs.open(new Path(hdfsFile));) {
            IOUtils.copyBytes(fis, outputStream, 4096, false);
        }
    }

    /**
     * 删除文件
     * @param file
     * @param isRecursive
     * @return
     * @throws IOException
     */
    public boolean delFile(String file, boolean isRecursive) throws IOException {
        Path path = new Path(file);
        boolean isDeleted = hdfs.delete(path, isRecursive);
        return isDeleted;
    }

    /**
     * 获取文件夹下的列表
     * @param dir
     * @return
     * @throws IOException
     */
    public FileStatus[] listFiles(String dir) throws IOException {
        return hdfs.listStatus(new Path(dir));
    }

    /**
     * 获取某一个文件的信息
     * @param file
     * @return
     * @throws IOException
     */
    public FileStatus getFileStatus(String file) throws IOException {
        return hdfs.getFileStatus(new Path(file));
    }

    /**
     * 检测文件或目录是否存在
     * @param file
     * @return
     * @throws IOException
     */
    public boolean checkExist(String file) throws IOException {
        Path path = new Path(file);
        return hdfs.exists(path);
    }

    /**
     * 获得在HDFS上文件所分的文件块所在的主机名
     * @param file    文件名称
     * @return 文件所分的文件块所在主机名称集合
     */
    public List<String[]> getFileBolckHost(String file) throws IOException {
        List<String[]> list = new ArrayList<String[]>();
        Path path = new Path(file);
        FileStatus fileStatus = hdfs.getFileStatus(path);

        BlockLocation[] blkLocations = hdfs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());

        int blkCount = blkLocations.length;
        for (int i = 0; i < blkCount; i++) {
            String[] hosts = blkLocations[i].getHosts();
            list.add(hosts);
        }
        return list;

    }

    /**
     * 获得所有datanode主机名
     * @return 所有datanode主机名称集合
     */
    public String[] getAllNodeName() throws IOException {
        DistributedFileSystem dfs = (DistributedFileSystem) hdfs;
        DatanodeInfo[] dataNodeStats = dfs.getDataNodeStats();
        String[] names = new String[dataNodeStats.length];
        for (int i = 0; i < dataNodeStats.length; i++) {
            names[i] = dataNodeStats[i].getHostName();
        }
        return names;
    }
}
