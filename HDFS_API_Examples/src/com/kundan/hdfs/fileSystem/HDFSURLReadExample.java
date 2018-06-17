package com.kundan.hdfs.fileSystem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class HDFSURLReadExample {

	static {
		/* setURLStreamHandlerFactory can be set only once per JVM, 
		so its set within static block, but this exposes the risk of 
		this can be set by third party.*/
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	public static void main(String[] args) throws IOException {
		
		InputStream is=null;
		try {
			is=new URL("hdfs://172.17.0.2:8020/user/root/foo.txt").openStream();
			IOUtils.copyBytes(is, System.out, 4096,false);
		}
		finally {
			IOUtils.closeStream(is);
		}
		
	}

}
