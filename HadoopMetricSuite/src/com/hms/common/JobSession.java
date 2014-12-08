package com.hms.common;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class JobSession {

	//User account
	public static String masterIP;
	public static String username;
	public static String password;
	
	//Job details
	public static int retrievalFrequency = 1;
	public static String applicationType;
	public static int expectedRuns;
	public static int currentRunNo = 1;
	public static int experimentNum = 1;
	
	//JobConfig table model
	public static String jobID;
	public static int nodes;
	public static double datasize;
	public static String startTime;
	public static String endTime;
	
	public static String hmsPath;
	
	//Slave
	public static Map<String, String> processIDOfSlaves = new HashMap<String, String>();

	public static void startUp()
	{
		File dir = new File("./");
		
		if (dir.listFiles() != null)
		{
			for(File file: dir.listFiles()) 
			{
				if (file.getName().endsWith(".log"))
				{
					file.delete();
				}
			}
		}
	}
	
	public static void cleanUp()
	{
		File dir = new File("./");
		
		if (dir.listFiles() != null)
		{
			for(File file: dir.listFiles()) 
			{
				if (file.getName().endsWith(".txt"))
				{
					file.delete();
				}
			}
		}
	}
	
	public static void exportResourcesFromJAR() throws Exception {
		
		File dbFile = new File(JobSession.getPathForResource("HadoopMetrics.sqlite"));
		
		if (dbFile.exists())
		{
			System.out.println("DB File exists, so do not replace db file");
		}
		else
		{
			JobSession.exportFile("HadoopMetrics.sqlite");
		}
		
		JobSession.exportFile("DataGenerator.jar");
		JobSession.exportFile("Default.html");
		JobSession.exportFile("NoXML.html");
		JobSession.exportFile("Platform.sh");
		JobSession.exportFile("Template.html");
		JobSession.exportFile("Template2.html");
		JobSession.exportFile("NoAppMetrics.html");
		JobSession.exportFile(Constants.APPLICATIONTYPES.get(Constants.WORD_COUNT));
    }
	
	private static void exportFile(String resourceName) throws Exception
	{
		InputStream ddlStream = JobSession.class
			    .getClassLoader().getResourceAsStream(resourceName);
			FileOutputStream fos = null;
			try {
			    fos = new FileOutputStream(JobSession.hmsPath + resourceName);
			    byte[] buf = new byte[2048];
			    int r = ddlStream.read(buf);
			    while(r != -1) {
			        fos.write(buf, 0, r);
			        r = ddlStream.read(buf);
			    }
			} finally {
			    if(fos != null) {
			        fos.close();
			    }
			}
	}
	
	public static String getPathForResource(String resourceName)
	{
		return JobSession.hmsPath + resourceName;
	}
	
	public static String getGraphPath()
	{
		return JobSession.hmsPath + "graph/";
	}
}
