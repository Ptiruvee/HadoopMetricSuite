package com.hms.common;

import java.util.HashMap;
import java.util.Map;

public class JobSession {

	//User account
	public static String masterIP;
	public static String username;
	public static String password;
	
	//Job details
	public static int retrievalFrequency = 1;
	
	//JobConfig table model
	public static String jobID;
	public static int nodes;
	public static double datasize;
	public static String startTime;
	public static String endTime;
	
	//Slave
	public static Map<String, String> processIDOfSlaves = new HashMap<String, String>();
}
