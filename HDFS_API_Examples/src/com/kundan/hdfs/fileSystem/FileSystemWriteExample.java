package com.kundan.hdfs.fileSystem;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class FileSystemWriteExample {
	
	public static void main(String args[]) throws IOException {
	String uri="hdfs://172.17.0.2:8020/user/root/bar.txt";
	Configuration conf = new Configuration();
	
	FileSystem fs = FileSystem.get(URI.create(uri),conf);
	System.out.println("Start writing your text, hit enter to end;");
	InputStream in = new ByteArrayInputStream(new Scanner(System.in).nextLine().getBytes());
	OutputStream out = null;
	try {
		out=fs.create(new Path(uri),new Progressable() {
			
			@Override
			public void progress() {
				// TODO Auto-generated method stub
				System.out.print("*");
			}
		});
		IOUtils.copyBytes(in, out, 4096,true);
	}
	catch (Exception e) {
		e.printStackTrace();
	}
	}

}
