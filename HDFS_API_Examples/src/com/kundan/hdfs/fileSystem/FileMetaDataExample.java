package com.kundan.hdfs.fileSystem;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;

public class FileMetaDataExample {
	
	public static void main(String[] args) throws IOException {
		String uri="/user/root/";
		Path path= new Path(uri);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri),conf);
		
		//Get Metatdata from file
		FileStatus fileStatus=fs.getFileStatus(new Path(uri));
		System.out.println(fileStatus.getPath());
		System.out.println(fileStatus.getBlockSize());
		System.out.println(fileStatus.getAccessTime());
		System.out.println(fileStatus.getGroup());
		System.out.println(fileStatus.getLen());
		System.out.println(fileStatus.getModificationTime());
		System.out.println(fileStatus.getOwner());
		System.out.println(fileStatus.getReplication());
		System.out.println(fileStatus.getPermission());
		System.out.println(fileStatus.isSymlink()?fileStatus.getSymlink():null);
		
		//Listing files in directory
		FileStatus [] filesList= fs.listStatus(new Path("/user/root/"));
		for(FileStatus file: filesList){
			System.out.println(file.getPath());
		}
		

	}

}
