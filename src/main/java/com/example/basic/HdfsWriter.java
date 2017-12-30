package com.example.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;

/**
 * Reference:
 * - http://hadoopinrealworld.com/writing-a-file-to-hdfs-java-program/
 */
public class HdfsWriter {
    private static Configuration conf;
    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsWriter.class);

    public static void main(String[] args) throws IOException {
        BasicConfigurator.configure();
        // Get configuration of Hadoop system
        conf = new Configuration();

        LOGGER.info("Connecting to {}", conf.get("fs.defaultFS"));


        writingData2HdfsFile("Hello world!!!", "output/new_text");
//        apppendData2HdfsFile("Hello world!!", "output/new_text2");
        localFile2Hdfs("src/main/resources/file/daily_show_guests", "output/daily_show_guests");
        skipLimitCharacters("src/main/resources/file/daily_show_guests", "output/daily_show_guests_skip", 100);
        srcHdfsFile2DstHdfsFile("output/daily_show_guests", "output/daily_show_guests_gbk");
    }


    /**
     * Writing data to hdfs file
     *
     * @param content Raw data
     * @param dstPath Destination file in HDFS
     * @throws IOException
     */
    static void writingData2HdfsFile(String content, String dstPath) throws IOException {
        try (
                FileSystem fs = FileSystem.get(conf);
                // Destination file in HDFS
                FSDataOutputStream fsos = fs.create(new Path(dstPath))
        ) {
            byte[] buffer = content.getBytes();
            fsos.write(buffer, 0, buffer.length);
        }
    }

    /**
     * Append data to hdfs file
     * @param content
     * @param dstPath
     * @throws IOException
     */
    static void apppendData2HdfsFile(String content, String dstPath) throws IOException {
        try (
                FileSystem fs = FileSystem.get(conf);
                FSDataOutputStream fsos = fs.append(new Path(dstPath))

        ) {
            byte[] buffer = content.getBytes();
            fsos.write(buffer);
        }
    }


    /**
     * Writing a Source hdfs File to Destination hdfs file
     *
     * @param srcPath Source file in HDFS
     * @param dstPath Destination file in HDFS
     * @throws IOException
     */
    static void srcHdfsFile2DstHdfsFile(String srcPath, String dstPath) throws IOException {
        try (
                FileSystem fs = FileSystem.get(conf);
                // Source file in HDFS
                FSDataInputStream fsis = fs.open(new Path(srcPath));
                BufferedReader br = new BufferedReader(new InputStreamReader(fsis, "UTF-8"));

                // Destination file in HDFS
                FSDataOutputStream fsos = fs.create(new Path(dstPath));
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fsos, Charset.forName("GBK")))
        ) {
            String line;
            while ((line = br.readLine()) != null) {
                bw.append(line).append(System.getProperty("line.separator"));
            }
        }
    }

    /**
     * Writing a file to Hdfs
     *
     * @param localSrc Source file in the local file system
     * @param hdfsDst  Destination file in HDFS
     * @throws IOException
     */
    static void localFile2Hdfs(String localSrc, String hdfsDst) throws IOException {

        try (
                FileSystem fs = FileSystem.get(conf);
                // Input stream for the file in local file system to be written to HDFS
                InputStream in = new BufferedInputStream(new FileInputStream(localSrc));

                // Destination file in HDFS
                OutputStream os = fs.create(new Path(hdfsDst))
        ) {
            // Copy file from local to HDFS
            IOUtils.copyBytes(in, os, 4096, true);

            LOGGER.info("{} copied to HDFS", hdfsDst);
        }
    }


    /**
     * @param localSrc Source file in the local file system
     * @param hdfsDst  Destination file in HDFS
     * @param limit    the number of bytes to be skipped
     * @throws IOException
     */
    static void skipLimitCharacters(String localSrc, String hdfsDst, long limit) throws IOException {
        try (
                FileSystem fs = FileSystem.get(conf);
                // Input stream for the file in local file system to be written to HDFS
                InputStream in = new BufferedInputStream(new FileInputStream(localSrc));

                // Destination file in HDFS
                OutputStream os = fs.create(new Path(hdfsDst))
        ) {
            in.skip(limit);

            byte[] buffer = new byte[20];

            // 从101的位置读取20个字符到buffer中
            int bytesRead = in.read(buffer);
            if (bytesRead >= 0) {
                os.write(buffer, 0, bytesRead);
            }
        }
    }
}
