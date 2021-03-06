package com.hms.common;

import java.io.File;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;

/**
 * @author adithya
 * @version 1.0
 *
 * This class is responsible to maintain variables related to an application run and test, i.e each time application is opened 
 * and whenever a test is run.
 *
 */

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
	
	public static String hmsPath = "";
	
	public static boolean isWindows = false;
	
	//Slave
	public static Map<String, String> processIDOfSlaves = new HashMap<String, String>();
	
	static final Logger log = (Logger) LogManager.getLogger(JobSession.class.getName());

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
			log.info("DB File exists, so do not replace db file");
		}
		else
		{
			JobSession.exportFile("HadoopMetrics.sqlite");
		}
		
		log.info("DB File exists, so do not replace db file1");
		JobSession.exportFile("DataGenerator.jar");
		
		log.info("DB File exists, so do not replace db file2");
		JobSession.exportFile("Default.html");
		
		log.info("DB File exists, so do not replace db file3");
		JobSession.exportFile("NoXML.html");
		
		log.info("DB File exists, so do not replace db file4");
		JobSession.exportFile("Platform.sh");
		
		log.info("DB File exists, so do not replace db file5");
		JobSession.exportFile("Template.html");
		
		log.info("DB File exists, so do not replace db file6");
		JobSession.exportFile("Template2.html");
		
		log.info("DB File exists, so do not replace db file7");
		JobSession.exportFile("NoAppMetrics.html");
		
		log.info("DB File exists, so do not replace db file8");
		JobSession.exportFile("jetty-runner-9.3.0.M0.jar");
		
		log.info("DB File exists, so do not replace db file9");
		JobSession.exportFile(Constants.APPLICATIONTYPES.get(Constants.WORD_COUNT));
		
		log.info("DB File exists, so do not replace db file10");
		JobSession.exportFile(Constants.APPLICATIONTYPES.get(Constants.GREP));
		
		log.info("DB File exists, so do not replace db file11");
		JobSession.exportFile(Constants.APPLICATIONTYPES.get(Constants.SORT));
		
		log.info("DB File exists, so do not replace db file12");
		JobSession.exportFile(Constants.APPLICATIONTYPES.get(Constants.DEDUP));
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
	
	public static void findOutOS()
	{
		log.info("OS " + System.getProperty("os.name"));
		
		if (System.getProperty("os.name").startsWith("Windows"))
		{
			JobSession.isWindows = true;
		}
	}
}
