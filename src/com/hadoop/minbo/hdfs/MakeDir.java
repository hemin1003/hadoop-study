package com.hadoop.minbo.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MakeDir {
	public static void main(String[] args) throws IOException {  
        Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(conf);  
        Path path = new Path("/user/local/hadoop/data/20130709");  
        fs.create(path);  
        fs.close();  
        
        System.out.println("done");
    }  
}
