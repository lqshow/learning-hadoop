package com.example.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetReader.Builder;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.tools.read.SimpleReadSupport;
import org.apache.parquet.tools.read.SimpleRecord;
import org.apache.parquet.example.data.Group;

public class Reader {
    private static Configuration conf;

    public static void main(String[] args) throws Exception {
        conf = new Configuration();

        String hdfsDst = "input/users.parquet";

        simpleRead(hdfsDst);
        groupRead(hdfsDst);
    }

    static void simpleRead(String hdfsDst) throws Exception {
        try (
                ParquetReader<SimpleRecord> currentReader = ParquetReader
                        .builder(new SimpleReadSupport(), new Path(hdfsDst))
                        .withConf(conf)
                        .build()
        ) {
            SimpleRecord record = null;
            while ((record = currentReader.read()) != null) {
                System.out.println(record);

                for (SimpleRecord.NameValue nameValue : record.getValues()) {
                    System.out.println(nameValue.getName() + ": " + nameValue.getValue());
                }
            }
        }
    }

    static void groupRead(String hdfsDst) throws Exception {
        GroupReadSupport readSupport = new GroupReadSupport();
        Builder<Group> builder = ParquetReader.builder(readSupport, new Path(hdfsDst));
        try (
                ParquetReader<Group> currentReader = builder.build()
        ) {
            Group line = null;
            while ((line = currentReader.read()) != null) {
                System.out.println(line);
            }
        }
    }
}
