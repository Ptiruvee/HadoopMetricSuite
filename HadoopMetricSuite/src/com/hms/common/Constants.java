package com.hms.common;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class Constants {
	
	//Error codes
	public static final Map<String, String> ERRORCODES;
    static {
        Map<String, String> aMap = new HashMap<String, String>(); 
        aMap.put("WrongMasterDetails", "Something is wrong, please check the ip address, username or password!");
        aMap.put("SuccessMasterConnection", "Connected successfully to master...");
        aMap.put("FailedMasterConnection", "Unable to connect to master, cannot proceed further!");
        aMap.put("NoMasterConnection", "Cannot fetch slave list without connection to master!");
        aMap.put("SuccessMasterSlaveListFetch", "Fetched slave list from master successfully...");
        aMap.put("FailedMasterSlaveListFetch", "Unable to fetch slave list from master successfully!");
        aMap.put("ClosedMasterConnection", "Closing connection with master...");
        
        aMap.put("ScriptUpload", "Uploaded script file...");
        aMap.put("ScriptExecutionSuccess", "Script file executed and running in background...");
        aMap.put("ScriptExecutionFailure", "Script file did not execute...");
        
        aMap.put("JarTransferSuccess", "Application jar files transferred...");
        
        aMap.put("JobAboutToRun", "job will start now...");
        aMap.put("JobCannotRun", "job cannot start now!");
        
        aMap.put("LogFileRead", "Log file is about to be read...");
        aMap.put("LogFileNoRead", "Unable to read Log file!");
        
        aMap.put("ScriptKillSuccess", "Stopped script running in the background...");
        aMap.put("ScriptKillFailure", "Unable to terminate script running in the background!");
        
        ERRORCODES = Collections.unmodifiableMap(aMap);
    }
    
    //Application type
    public static final Map<String, String> APPLICATIONTYPES;
    static {
        Map<String, String> aMap = new HashMap<String, String>(); 
        aMap.put("WordCount", "WordCount.jar");
        aMap.put("Sort", "Sort.jar");
        aMap.put("Grep", "Grep.jar");
        aMap.put("Dedup", "Dedup.jar");
        
        APPLICATIONTYPES = Collections.unmodifiableMap(aMap);
    }
    
    //Application name
    public static final String WORD_COUNT = "WordCount";
    public static final String SORT = "Sort";
    public static final String Grep = "Grep";
    public static final String Dedup = "Dedup";
    
	//User path
	public static final String USER_PATH = "/home/ec2-user/";
	
	//Script
	public static final String SCRIPT_PATH = "scripts/";
	public static final String SCRIPT_NAME = "CPU.sh";
	
	//Application
	public static final String JAR_PATH = "applications/";
	
	//Log
	public static final String TEMP_LOG_NAME = "temp.txt";
	public static final String CPU_LOG_NAME = "hms_cpu.txt";
	
	//Hadoop version & path
	public static final String HADOOP_VERSION = "hadoop-2.5.0";
	public static final String HADOOP_BIN = "/bin/hadoop";
	public static final String SLAVE_LIST_PATH = "/etc/hadoop/slaves";

	//Boundary
	public static final int MAXIMUM_SLAVES = 100;
}
