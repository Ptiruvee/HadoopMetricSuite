package com.hms.common;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Constants {
	
	//Error codes
	public static final Map<String, String> errorCodes;
    static {
        Map<String, String> aMap = new HashMap<String, String>(); 
        aMap.put("WrongMasterDetails", "Something is wrong, please check the ip address, username or password!");
        aMap.put("SuccessMasterConnection", "Connected successfully to master...");
        aMap.put("FailedMasterConnection", "Unable to connect to master, cannot proceed further!");
        aMap.put("NoMasterConnection", "Cannot fetch slave list without connection to master!");
        aMap.put("SuccessMasterSlaveListFetch", "Fetched slave list from master successfully...");
        aMap.put("FailedMasterSlaveListFetch", "Unable to fetch slave list from master successfully!");
        aMap.put("ClosedMasterConnection", "Closing connection with master...");
        
        errorCodes = Collections.unmodifiableMap(aMap);
    }
    
	//User path
	public static final String USER_PATH = "/home/ec2-user/";
	
	//Hadoop version & path
	public static final String HADOOP_VERSION = "hadoop-2.5.0";
	public static final String SLAVE_LIST_PATH = "/etc/hadoop/slaves";

	//Boundary
	public static final int MAXIMUM_SLAVES = 100;
}
