package com.hms.common;

public class Constants {
	
	//Error codes
	public static final String NO_MASTER_CONNECTION = "100";
	public static final String NO_SLAVE_LIST = "101";
	
	//User path
	public static final String USER_PATH = "/home/ec2-user/";
	
	//Hadoop version & path
	public static final String HADOOP_VERSION = "hadoop-2.5.0";
	public static final String SLAVE_LIST_PATH = "/etc/hadoop/slaves";

	//Boundary
	public static final int MAXIMUM_SLAVES = 100;
}
