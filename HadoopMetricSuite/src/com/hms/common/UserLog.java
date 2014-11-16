package com.hms.common;

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
