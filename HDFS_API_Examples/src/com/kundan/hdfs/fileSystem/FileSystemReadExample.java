package com.kundan.hdfs.fileSystem;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileSystemReadExample {

	public static void main(String[] args) throws IOException{
		String uri="hdfs://172.17.0.2:8020/user/root/foo.txt";
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(URI.create(uri),conf);
		FSDataInputStream in=null;
		byte[] buffer=new byte[4096];
		try {
			// Print the entire File
				in=fs.open(new Path(uri));
				IOUtils.copyBytes(in,System.out,4096,false);
			
			/* Read from stream into buffer which start with position and selects
			between offset and length*/
			
				int r=in.read(90,buffer,0,6);
				System.out.println("Read return"+r);
				System.out.println("\n\nData Buffered "+ new String(buffer).toString());


			//Seek from given offset
				in.seek(50);
				IOUtils.copyBytes(in,System.out,4096,false);
			
		}
		finally {
			IOUtils.closeStream(in);
		}
		

	}

}
