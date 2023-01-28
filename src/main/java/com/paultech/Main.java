package com.paultech;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class Main {
    public static void main(String[] args) throws Exception {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://10.180.210.228:8020/"), new Configuration(), "hdfs");

        fileSystem.createNewFile(new Path("/test.txt"));
        FSDataOutputStream fsDataOutputStream = fileSystem.append(new Path("/test.txt"));
        fsDataOutputStream.writeChars("test");
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
        fileSystem.close();
    }
}