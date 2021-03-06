package com.example.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.io.*;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class HdfsReader {
    private static Configuration conf;

    public static void main(String[] args) throws Exception {
        // Get configuration of Hadoop system
        conf = new Configuration();

        fileSystemCat("output/daily_show_guests");
        fileReadFromHdfs("output/daily_show_guests", "/Users/linqiong/Downloads/daily_show_guests");
    }

    /**
     * fs cat
     *
     * @param srcPath
     * @throws Exception
     */
    static void fileSystemCat(String srcPath) throws Exception {
        FSDataInputStream fsis = null;
        try (FileSystem fs = FileSystem.get(conf)) {
            Path inputPath = new Path(srcPath);
            if (!fs.exists(inputPath)) {
                throw new Exception("File does not exist");
            }

            fsis = fs.open(inputPath);
            IOUtils.copyBytes(fsis, System.out, 4096, false);

            System.out.println("End Of file: HDFS file read complete");
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            IOUtils.closeStream(fsis);
        }
    }

    /**
     * Reading a hdfs file to local file
     *
     * @param srcPath
     * @param localOutputPath
     */
    static void fileReadFromHdfs(String srcPath, String localOutputPath) throws Exception {
        try (
                FileSystem fs = FileSystem.get(conf);
                FSDataInputStream fsis = fs.open(new Path(srcPath));
                OutputStream os = new BufferedOutputStream(new FileOutputStream(localOutputPath))
        ) {
            IOUtils.copyBytes(fsis, os, conf);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
