package com.hms.common;

import java.util.Map;

public class JobSession {

	//User account
	public static String username;
	public static String password;
	
	//JobConfig table model
	public static String jobID;
	public static int nodes;
	public static double datasize;
	public static String startTime;
	public static String endTime;
	
	//Slave
	public static Map<String, String> processIDOfSlaves;
}
