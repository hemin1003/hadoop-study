package com.hadoop.minbo.hdfs;

import java.io.IOException;  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FSDataOutputStream;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;  

public class WriteFile {
	public static void main(String[] args) throws IOException {  
        Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(conf);  
        Path path = new Path("/user/hadoop/write.txt");  
        FSDataOutputStream out = fs.create(path);
//        out.writeUTF("da jia hao,cai shi zhen de hao!");  
        out.writeChars("hemin");
        out.flush();
        fs.close();  
        
        System.out.println("done");
    }  
}
