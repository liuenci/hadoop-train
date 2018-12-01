package com.imooc.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;


public class HDFSApp {

    public static final String HDFS_PATH = "hdfs://192.168.111.129:8020";
    FileSystem fileSystem = null;
    Configuration configuration = null;

    @Before
    public void setUp() throws Exception {
        System.out.println("HDFSApp.setUp");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "hadoop");
    }

    @After
    public void tearDown() throws Exception {
        configuration = null;
        fileSystem = null;

        System.out.println("HDFSApp.tearDown");
    }

    /**
     * 创建HDFS目录
     */
    @Test
    public void mkdir() throws IOException {
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    /**
     * 创建文件
     */
    @Test
    public void create() throws Exception {
        FSDataOutputStream outputStream = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        outputStream.write("hello hadoop".getBytes());
        outputStream.flush();
        outputStream.close();
    }
    /**
     * 查看HDFS文件的内容
     */
    @Test
    public void cat() throws Exception{
        FSDataInputStream inputStream = fileSystem.open(new Path("/hdfsapi/test/a.txt"));
        IOUtils.copyBytes(inputStream,System.out,1024);
        inputStream.close();
    }

    /**
     * 重命名
     */
    @Test
    public void rename() throws Exception{
        Path oldPath = new Path("/hdfsapi/test/a.txt");
        Path newPath = new Path("/hdfsapi/test/b.txt");
        fileSystem.rename(oldPath,newPath);
    }

    /**
     * 上传文件到 HDFS
     * @throws Exception
     */
    @Test
    public void copyFromLocalFile() throws Exception{
        Path localPath = new Path("D:\\百度云盘\\64、10小时入门大数据\\第3章 分布式文件系统HDFS\\第3章 分布式文件系统HDFS.mp4");
        Path hdfsPath = new Path("/hdfsapi/test/1.mp4");
        fileSystem.copyFromLocalFile(localPath,hdfsPath);
    }
    /**
     * 上传文件到 HDFS
     */
    @Test
    public void copyFromLocalFileWithProgress() throws Exception{
        InputStream inputStream = new BufferedInputStream(new FileInputStream(new File("D:\\百度云盘\\64、10小时入门大数据\\第3章 分布式文件系统HDFS\\第3章 分布式文件系统HDFS.mp4")));
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/百度云.mp4"), new Progressable() {
            @Override
            public void progress() {
                System.out.print(".");
            }
        });
        IOUtils.copyBytes(inputStream,output,4096);
    }

    /**
     * 下载 HDFS 文件
     * @throws Exception
     */
    @Test
    public void copyToLocalFile() throws Exception {
        Path localPath = new Path("D:/1.txt");
        Path hdfsPath = new Path("/hello.txt");
        fileSystem.copyToLocalFile(hdfsPath, localPath);
    }
    @Test
    public void listFiles() throws Exception {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));

        for (FileStatus fileStatus : fileStatuses) {
            String isDir = fileStatus.isDirectory() ? "文件夹" : "文件";
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();
            System.out.println(isDir + "\t" + replication + "\t" + len + "\t" + path);
        }
    }
    @Test
    public void delete() throws Exception {
        fileSystem.delete(new Path("/test"),true);
    }
}
