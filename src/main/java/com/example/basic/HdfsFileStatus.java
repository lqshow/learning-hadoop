package com.example.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * References:
 * - https://programtalk.com/java-api-usage-examples/org.apache.hadoop.fs.FileStatus/
 */
public class HdfsFileStatus {
    private static Configuration conf;

    public static void main(String[] args) throws Exception {
        conf = new Configuration();

        getSizeAndLastModified("output");
    }

    static Pair getSizeAndLastModified(String path) throws IOException {
        try (
                FileSystem fs = FileSystem.get(conf)
        ) {
            // Get all contained files if path is directory
            ArrayList<FileStatus> allFiles = new ArrayList<>();


            FileStatus status = fs.getFileStatus(new Path(path));
            if (status.isFile()) {
                allFiles.add(status);
            } else {
                FileStatus[] listStatus = fs.listStatus(new Path(path));
                if (listStatus != null) {
                    allFiles.addAll(Arrays.asList(listStatus));
                }
            }
            long size = 0;
            long lastModified = 0;
            for (FileStatus file : allFiles) {
                size += file.getLen();
                lastModified = Math.max(lastModified, file.getModificationTime());
            }

            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String modification = format.format(lastModified);
            System.out.println(size + " " + lastModified + " " + modification);
            /**
             * output
             *
             * 314592 1514647119032 2017-12-30 23:18:39
             */

            return Pair.newPair(size, lastModified);
        }
    }
}
