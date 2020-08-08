package com.example.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * https://www.cnblogs.com/seaspring/articles/6523395.html
 * http://blog.csdn.net/woloqun/article/details/76068147
 * https://www.cnblogs.com/yanghaolie/p/7156372.html
 * https://github.com/apache/parquet-mr/blob/master/parquet-column/src/test/java/org/apache/parquet/io/TestColumnIO.java
 * https://www.programcreek.com/java-api-examples/index.php?api=parquet.hadoop.ParquetReader
 */
public class Writer {
    public static void main(String[] args) {
        Configuration conf = new Configuration();

//        ParquetWriter<Group> parquetWriter = new ParquetWriter<>(
//                new Path(outputDir, filename),
//                writeSupport,
//                CompressionCodecName.SNAPPY,
//                ParquetWriter.DEFAULT_BLOCK_SIZE,
//                ParquetWriter.DEFAULT_PAGE_SIZE,
//                ParquetWriter.DEFAULT_PAGE_SIZE,
//                ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
//                ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
//                ParquetProperties.WriterVersion.PARQUET_1_0,
//                conf);
    }
}
