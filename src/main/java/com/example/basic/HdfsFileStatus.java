package com.example.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Get fs metadata
 */
public class HdfsFileStatus {
    private static Configuration conf;

    public static void main(String[] args) throws IOException {
        conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path path = new Path("output/daily_show_guests");
        FileStatus fss = fs.getFileStatus(path);
        System.out.println(fss);
    }
}
