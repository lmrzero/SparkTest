package cn.edu360.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * created by LMR on 2019/6/3
 */
public class HDFSCreate {

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path file = new Path("hdfs://127.0.0.1:9000/example.txt");
        FSDataOutputStream outStream = fs.create(file);
        outStream.write("java api write data".getBytes("UTF-8"));
        outStream.close();
    }
}
