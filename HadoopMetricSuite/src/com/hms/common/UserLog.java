package com.hms.common;

/**
 * @author adithya
 * @version 1.0
 *
 * This class is responsible to collect and hold logs from ClusterMaster/ClusterSlave class to display it to the user. It will be
 * accessed and flushed by ConfigurationScreen.
 *
 */

public class UserLog {

	private static StringBuilder logMessage = new StringBuilder();
	
	public static void addToLog(String log)
	{
		logMessage.append(log);
		logMessage.append("\n");
	}
	
	public static String getUserLog()
	{
		String message = logMessage.toString();
		logMessage.setLength(0);
		return message;
	}
}
