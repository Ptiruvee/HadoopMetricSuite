package com.hms.connection;

import java.io.PrintWriter;
import java.util.Date;

import net.neoremind.sshxcute.core.ConnBean;
import net.neoremind.sshxcute.core.Result;
import net.neoremind.sshxcute.core.SSHExec;
import net.neoremind.sshxcute.exception.TaskExecFailException;
import net.neoremind.sshxcute.task.CustomTask;
import net.neoremind.sshxcute.task.impl.ExecCommand;
import net.neoremind.sshxcute.task.impl.ExecShellScript;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;

import com.hms.common.Constants;
import com.hms.common.JobSession;
import com.hms.common.UserLog;
import com.hms.database.DatabaseManager;

public class ClusterMaster {

	private SSHExec sshMaster = null;
	private String[] slaveAddress = null;
	private String nodeID;

	private DatabaseManager dbManager = new DatabaseManager();

	static final Logger log = (Logger) LogManager.getLogger(ClusterMaster.class.getName());

	public boolean connectToMaster(String ipAddress, String username, String password)
	{
		if (!(ipAddress.length() > 0 && username.length() > 0 && password.length() > 0))
		{
			UserLog.addToLog(Constants.ERRORCODES.get("WrongMasterDetails"));
			log.error(Constants.ERRORCODES.get("WrongMasterDetails"));

			return false;
		}

		try {

			ConnBean cb = new ConnBean(ipAddress, username, password);

			sshMaster = SSHExec.getInstance(cb);      

			if (sshMaster.connect())
			{
				nodeID = ipAddress;

				UserLog.addToLog(Constants.ERRORCODES.get("SuccessMasterConnection") + nodeID);
				log.info(Constants.ERRORCODES.get("SuccessMasterConnection") + nodeID);

				return true;
			}

			UserLog.addToLog(Constants.ERRORCODES.get("FailedMasterConnection"));
			log.info(Constants.ERRORCODES.get("FailedMasterConnection"));

			return false;

		} catch (Exception e) {

			UserLog.addToLog(Constants.ERRORCODES.get("FailedMasterConnection"));
			log.error("Master connection exception", e);

			return false;
		} 
	}

	public boolean fetchSlaveList()
	{
		if (sshMaster == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoMasterConnection"));
			log.error(Constants.ERRORCODES.get("NoMasterConnection"));

			return false;
		}

		try {

			UserLog.addToLog(Constants.ERRORCODES.get("MasterSlaveListFetch"));
			log.info(Constants.ERRORCODES.get("MasterSlaveListFetch"));

			CustomTask catCmd = new ExecCommand("cat " + Constants.USER_PATH + Constants.HADOOP_VERSION + Constants.SLAVE_LIST_PATH);

			Result catCmdRes = sshMaster.exec(catCmd);

			if (catCmdRes.isSuccess)
			{
				UserLog.addToLog(Constants.ERRORCODES.get("SuccessMasterSlaveListFetch"));
				log.info(Constants.ERRORCODES.get("SuccessMasterSlaveListFetch"));

				slaveAddress = catCmdRes.sysout.split("\n");

				return true;
			}                        
			else
			{
				UserLog.addToLog(Constants.ERRORCODES.get("FailedMasterSlaveListFetch"));
				log.info(Constants.ERRORCODES.get("FailedMasterSlaveListFetch"));
			}
		} catch (TaskExecFailException e) {
			log.error("Slave list exception", e);
		} catch (Exception e) {
			log.error("Slave list exception", e);
		} 

		return false;
	}

	public boolean transferAndRunScriptFile()
	{
		if (sshMaster == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoMasterConnection"));
			log.error(Constants.ERRORCODES.get("NoMasterConnection"));

			return false;
		}

		try {

			UserLog.addToLog(Constants.ERRORCODES.get("ScriptUpload"));
			log.info(Constants.ERRORCODES.get("ScriptUpload"));
			
			sshMaster.uploadSingleDataToServer(JobSession.getPathForResource(Constants.SCRIPT_NAME), Constants.USER_PATH + Constants.SCRIPT_NAME);

			CustomTask scriptPermission = new ExecCommand("chmod 755 " + Constants.USER_PATH + Constants.SCRIPT_NAME);
			sshMaster.exec(scriptPermission);
			
			CustomTask shellMaster = new ExecShellScript(Constants.USER_PATH.substring(0, Constants.USER_PATH.length()-1), "./" + Constants.SCRIPT_NAME + " " + JobSession.retrievalFrequency + " >> dummy.txt &", "");
			
			Result resMaster = sshMaster.exec(shellMaster);

			if (resMaster.isSuccess)
			{
				UserLog.addToLog(Constants.ERRORCODES.get("ScriptExecutionSuccess"));
				log.info(Constants.ERRORCODES.get("ScriptExecutionSuccess"));

				return true;
			}                        
			else
			{
				UserLog.addToLog(Constants.ERRORCODES.get("ScriptExecutionFailure"));
				log.info(Constants.ERRORCODES.get("ScriptExecutionFailure"));
			}
		} catch (TaskExecFailException e) {
			log.error("Master script execution exception", e);
		} catch (Exception e) {
			log.error("Master script execution exception", e);
		} 

		return false;
	}

	private boolean transferDataGenerationJARFile()
	{
		if (sshMaster == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoMasterConnection"));
			log.error(Constants.ERRORCODES.get("NoMasterConnection"));

			return false;
		}

		try
		{
			sshMaster.uploadSingleDataToServer(JobSession.getPathForResource(Constants.APPLICATIONTYPES.get("DataGen")), Constants.USER_PATH + Constants.APPLICATIONTYPES.get("DataGen"));

			UserLog.addToLog(Constants.ERRORCODES.get("DGJarTransferSuccess"));
			log.info(Constants.ERRORCODES.get("DGJarTransferSuccess"));

			return true;
		}
		catch (Exception e)
		{
			log.error("Master data generation jar transfer execution exception", e);
		}

		return false;
	}

	private boolean runDataGenerator()
	{
		if (sshMaster == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoMasterConnection"));
			log.error(Constants.ERRORCODES.get("NoMasterConnection"));

			return false;
		}

		if (!cleanUPHDFS())
		{
			return false;
		}

		try {
			String applicationPath = Constants.USER_PATH + Constants.APPLICATIONTYPES.get("DataGen");

			CustomTask runJob = new ExecCommand("java -jar " + applicationPath + " " + JobSession.datasize);

			UserLog.addToLog(Constants.ERRORCODES.get("DGJobAboutToRun"));
			log.info(Constants.ERRORCODES.get("DGJobAboutToRun"));

			Result res = sshMaster.exec(runJob);

			if (res.isSuccess)
			{
				//Job started running
				UserLog.addToLog(Constants.ERRORCODES.get("DGJobSuccess"));
				log.info(Constants.ERRORCODES.get("DGJobSuccess"));

				return true;
			}                        
			else
			{
				UserLog.addToLog(Constants.ERRORCODES.get("DGJobCannotToRun"));
				log.info(Constants.ERRORCODES.get("DGJobCannotToRun"));
			}
		} catch (TaskExecFailException e) {
			log.error("DG Job execution exception", e);
		} catch (Exception e) {
			log.error("DG Job execution exception", e);
		}

		return false;
	}

	private boolean cleanUPHDFS()
	{
		if (sshMaster == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoMasterConnection"));
			log.error(Constants.ERRORCODES.get("NoMasterConnection"));

			return false;
		}

		try {
			String hadoopInputDeletePath = Constants.USER_PATH + Constants.HADOOP_VERSION + Constants.HADOOP_BIN + " fs -rm -R /home/user/input";
			String hadoopOutputDeletePath = Constants.USER_PATH + Constants.HADOOP_VERSION + Constants.HADOOP_BIN + " fs -rm -R /home/user/output";
			String hadoopInputCreatePath = Constants.USER_PATH + Constants.HADOOP_VERSION + Constants.HADOOP_BIN + " fs -mkdir -p /home/user/input";

			CustomTask inputDeleteJob = new ExecCommand(hadoopInputDeletePath);

			sshMaster.exec(inputDeleteJob);

			CustomTask outputDeleteJob = new ExecCommand(hadoopOutputDeletePath);

			sshMaster.exec(outputDeleteJob);

			CustomTask inputCreateJob = new ExecCommand(hadoopInputCreatePath);

			Result res = sshMaster.exec(inputCreateJob);

			if (res.isSuccess)
			{
				//Job started running
				UserLog.addToLog(Constants.ERRORCODES.get("DataCleanedUp"));
				log.info(Constants.ERRORCODES.get("DataCleanedUp"));

				return true;
			}                        
			else
			{
				log.info(Constants.ERRORCODES.get("DataNotCleanedUp"));
			}

		} catch (TaskExecFailException e) {
			log.error("DG Job execution exception", e);
		} catch (Exception e) {
			log.error("DG Job execution exception", e);
		}

		return false;
	}

	private boolean transferInputFilesToHDFS()
	{
		if (sshMaster == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoMasterConnection"));
			log.error(Constants.ERRORCODES.get("NoMasterConnection"));

			return false;
		}

		try {
			String localPath = Constants.USER_PATH + "DataFile.txt";
			String hdfsCopyCmd = Constants.USER_PATH + Constants.HADOOP_VERSION + Constants.HADOOP_BIN + " fs -moveFromLocal " + localPath +  " /home/user/input/";

			CustomTask runJob = new ExecCommand(hdfsCopyCmd);

			UserLog.addToLog(Constants.ERRORCODES.get("LocalToHDFS"));
			log.info(Constants.ERRORCODES.get("LocalToHDFS"));

			Result res = sshMaster.exec(runJob);

			if (res.isSuccess)
			{
				//Job started running
				UserLog.addToLog(Constants.ERRORCODES.get("LocalToHDFSSuccess"));
				log.info(Constants.ERRORCODES.get("LocalToHDFSSuccess"));

				return true;
			}                        
			else
			{
				UserLog.addToLog(Constants.ERRORCODES.get("LocalToHDFSFailure"));
				log.info(Constants.ERRORCODES.get("LocalToHDFSFailure"));
			}
		} catch (TaskExecFailException e) {
			log.error("HDFS migration Job execution exception", e);
		} catch (Exception e) {
			log.error("HDFS migration Job execution exception", e);
		}

		return false;
	}

	private void transferApplicationJARFile(String type)
	{
		if (sshMaster == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoMasterConnection"));
			log.error(Constants.ERRORCODES.get("NoMasterConnection"));
		}

		try
		{
			sshMaster.uploadSingleDataToServer(JobSession.getPathForResource(Constants.APPLICATIONTYPES.get(type)), Constants.USER_PATH + Constants.APPLICATIONTYPES.get(type));

			UserLog.addToLog(Constants.ERRORCODES.get("JarTransferSuccess"));
			log.info(Constants.ERRORCODES.get("JarTransferSuccess"));
		}
		catch (Exception e)
		{
			log.error("Master application jar transfer execution exception", e);
		}
	}

	public boolean runApplicationJob(String type)
	{
		if (sshMaster == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoMasterConnection"));
			log.error(Constants.ERRORCODES.get("NoMasterConnection"));

			return false;
		}

		boolean jobResult = false;

		//transferApplicationJARFile
		if (type.equalsIgnoreCase(Constants.WORD_COUNT))
		{
			transferApplicationJARFile(type);
		}

		if (transferDataGenerationJARFile())
		{
			if (runDataGenerator())
			{
				if (transferInputFilesToHDFS())
				{
					if (startUpSlaves())
					{
						JobSession.startTime = fetchTime();

						try {
							String hadoopPath = Constants.USER_PATH + Constants.HADOOP_VERSION + Constants.HADOOP_BIN + " jar ";
							String applicationPath = "/home/ec2-user/hadoop-2.5.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.5.0.jar wordcount"; 

							if (type.equalsIgnoreCase(Constants.WORD_COUNT))
							{
								applicationPath = Constants.USER_PATH + Constants.APPLICATIONTYPES.get(type) + " " + Constants.WORD_COUNT;
							}
							else if (type.equalsIgnoreCase(Constants.GREP))
							{
								applicationPath = "/home/ec2-user/hadoop-2.5.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.5.0.jar grep";
							}
							else if (type.equalsIgnoreCase(Constants.SORT))
							{
								//applicationPath = "/home/ec2-user/hadoop-2.5.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.5.0.jar wordcount";
							}
							else if (type.equalsIgnoreCase(Constants.DEDUP))
							{
								//applicationPath = "/home/ec2-user/hadoop-2.5.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.5.0.jar wordcount";
							}

							String finalPath = hadoopPath + applicationPath + Constants.INPUT_PATH + Constants.OUTPUT_PATH;

							if (type.equalsIgnoreCase(Constants.GREP))
							{
								finalPath += " " + Constants.GREP_SEARCH_WORD;
							}

							CustomTask runJob = new ExecCommand(finalPath);

							UserLog.addToLog("Hadoop run command " + finalPath);
							log.info("Hadoop run command " + finalPath);

							UserLog.addToLog(Constants.ERRORCODES.get("JobAboutToRun"));
							log.info(Constants.ERRORCODES.get("JobAboutToRun"));

							Result res = sshMaster.exec(runJob);

							if (res.isSuccess)
							{
								jobResult = true;

								JobSession.endTime = fetchTime();

								readLogFile();

								readConfigFile();
								
								readAppMetrics();
							}                        
							else
							{
								UserLog.addToLog(Constants.ERRORCODES.get("JobCannotRun"));
								log.info(Constants.ERRORCODES.get("JobCannotRun"));
							}
						} catch (TaskExecFailException e) {
							log.error("Job execution exception", e);
						} catch (Exception e) {
							log.error("Job execution exception", e);
						}
						
						if (!jobResult)
						{
							killScriptRun();
							cleanUpLogs();

							if (slaveAddress != null)
							{
								cleanUpSlaves();
							}
						}
					}
					else
					{
						UserLog.addToLog(Constants.ERRORCODES.get("HadoopRunImpossible"));
						log.error(Constants.ERRORCODES.get("HadoopRunImpossible"));
					}
				}
				else
				{
					UserLog.addToLog(Constants.ERRORCODES.get("HadoopRunImpossible"));
					log.error(Constants.ERRORCODES.get("HadoopRunImpossible"));
				}
			}
			else
			{
				UserLog.addToLog(Constants.ERRORCODES.get("HadoopRunImpossible"));
				log.error(Constants.ERRORCODES.get("HadoopRunImpossible"));
			}
		}
		else
		{
			UserLog.addToLog(Constants.ERRORCODES.get("HadoopRunImpossible"));
			log.error(Constants.ERRORCODES.get("HadoopRunImpossible"));
		}

		return jobResult;
	}

	private String fetchTime()
	{
		try {
			CustomTask killJob = new ExecCommand("date +%s");

			Result res = sshMaster.exec(killJob);

			if (res.isSuccess)
			{
				if (res.sysout.trim().length() > 0)
				{
					return res.sysout.trim();
				}
				else
				{
					Date currentDate = new Date();
					return "" + currentDate.getTime();
				}
			}                        
			else
			{
				Date currentDate = new Date();
				return "" + currentDate.getTime();
			}
		} catch (TaskExecFailException e) {
			log.error("Start date fetch exception", e);
		} catch (Exception e) {
			log.error("Start date fetch exception", e);
		}

		Date currentDate = new Date();
		return "" + currentDate.getTime();
	}

	private void killScriptRun()
	{
		if (sshMaster == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoMasterConnection"));
			log.error(Constants.ERRORCODES.get("NoMasterConnection"));
		}

		try {
			CustomTask killJob = new ExecCommand("pkill " + Constants.SCRIPT_NAME);

			Result res = sshMaster.exec(killJob);

			if (res.isSuccess)
			{
				CustomTask killJob1 = new ExecCommand("pkill vmstat");
				sshMaster.exec(killJob1);
				
				UserLog.addToLog(Constants.ERRORCODES.get("ScriptKillSuccess"));
				log.info(Constants.ERRORCODES.get("ScriptKillSuccess"));
			}                        
			else
			{
				UserLog.addToLog(Constants.ERRORCODES.get("ScriptKillFailure"));
				log.info(Constants.ERRORCODES.get("ScriptKillFailure"));
			}
		} catch (TaskExecFailException e) {
			log.error("Script termination exception", e);
		} catch (Exception e) {
			log.error("Script termination exception", e);
		}
	}

	private void cleanUpLogs()
	{
		if (sshMaster == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoMasterConnection"));
			log.error(Constants.ERRORCODES.get("NoMasterConnection"));
		}

		try {
			CustomTask cleanJob = new ExecCommand("rm hms_*");

			Result res = sshMaster.exec(cleanJob);

			if (res.isSuccess)
			{
				log.info(Constants.ERRORCODES.get("LogFileCleaned"));
			}                        
			else
			{
				log.info(Constants.ERRORCODES.get("LogFileNotCleaned"));
			}

		} catch (TaskExecFailException e) {
			log.error("Script termination exception", e);
		} catch (Exception e) {
			log.error("Script termination exception", e);
		}
	}

	private void readLogFile()
	{
		if (sshMaster == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoMasterConnection"));
			log.error(Constants.ERRORCODES.get("NoMasterConnection"));
		}

		killScriptRun();

		JobSession.nodes = slaveAddress.length;
		
		UserLog.addToLog(Constants.ERRORCODES.get("AboutToReadLogFile"));
		log.info(Constants.ERRORCODES.get("AboutToReadLogFile"));

		if (readLog(Constants.CPU_LOG_NAME) && readLog(Constants.DISK_LOG_NAME) && readLog(Constants.MEM_LOG_NAME) && readLog(Constants.NET_LOG_NAME))
		{
			try
			{
				dbManager.getConnection();
				
				if (JobSession.expectedRuns > 1)
				{
					JobSession.jobID = JobSession.experimentNum + Constants.DELIMITER + JobSession.currentRunNo + Constants.DELIMITER + Constants.APPLICATIONCODES.get(JobSession.applicationType);
				}
				else
				{
					JobSession.jobID = JobSession.experimentNum + Constants.DELIMITER + Constants.APPLICATIONCODES.get(JobSession.applicationType);
				}
				
				dbManager.insertIntoJobConfig();

				UserLog.addToLog("Job ID " + JobSession.jobID);

				dbManager.insertIntoPlatformMetrics(nodeID);
			}
			catch (Exception e)
			{
				UserLog.addToLog(Constants.ERRORCODES.get("DBMasterInsertionError"));
				log.error(Constants.ERRORCODES.get("DBMasterInsertionError"), e);
			}

			dbManager.closeConnection();
		}

		cleanUpLogs();

		fetchFromSlaves();
	}

	private boolean readLog(String logFileName)
	{
		if (sshMaster == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoMasterConnection"));
			log.error(Constants.ERRORCODES.get("NoMasterConnection"));
		}

		try {
			CustomTask grep = new ExecCommand("cat " + Constants.USER_PATH + logFileName);

			Result res = sshMaster.exec(grep);

			if (res.isSuccess)
			{
				PrintWriter logOutput = new PrintWriter(nodeID + Constants.TEMP_LOG_NAME + logFileName);
				logOutput.println(res.sysout);
				logOutput.close();

				return true;
			}                        
			else
			{
				UserLog.addToLog(Constants.ERRORCODES.get("LogFileNoRead"));
				log.info(Constants.ERRORCODES.get("LogFileNoRead"));
			}
		} catch (TaskExecFailException e) {
			log.error("Read log file exception", e);
		} catch (Exception e) {
			log.error("Read log file exception", e);
		}

		return false;
	}

	private boolean readConfigFile()
	{
		if (sshMaster == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoMasterConnection"));
			log.error(Constants.ERRORCODES.get("NoMasterConnection"));
		}

		try {
			CustomTask grep = new ExecCommand("cat " + Constants.USER_PATH + Constants.HADOOP_VERSION + Constants.HADOOP_CONF_PATH);

			Result res = sshMaster.exec(grep);

			if (res.isSuccess)
			{
				UserLog.addToLog(Constants.ERRORCODES.get("ConfigFileRead"));
				log.info(Constants.ERRORCODES.get("ConfigFileRead"));

				PrintWriter logOutput = new PrintWriter(JobSession.getGraphPath() + JobSession.jobID + Constants.HADOOP_CONF_FILE);
				logOutput.println(res.sysout);
				logOutput.close();

				return true;
			}                        
			else
			{
				UserLog.addToLog(Constants.ERRORCODES.get("ConfigFileNoRead"));
				log.info(Constants.ERRORCODES.get("ConfigFileNoRead"));
			}
		} catch (TaskExecFailException e) {
			log.error("Read config file exception", e);
		} catch (Exception e) {
			log.error("Read config file exception", e);
		}

		return false;
	}
	
	private boolean readAppMetrics()
	{
		if (sshMaster == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoMasterConnection"));
			log.error(Constants.ERRORCODES.get("NoMasterConnection"));
		}

		try {
			CustomTask grep = new ExecCommand("cat " + Constants.USER_PATH + Constants.APP_LOG_NAME);

			Result res = sshMaster.exec(grep);

			if (res.isSuccess)
			{
				UserLog.addToLog(Constants.ERRORCODES.get("AppMetricsFileRead"));
				log.info(Constants.ERRORCODES.get("AppMetricsFileRead"));

				PrintWriter logOutput = new PrintWriter(JobSession.getGraphPath() + JobSession.jobID + Constants.APP_LOG_NAME);
				logOutput.println(res.sysout);
				logOutput.close();

				return true;
			}                        
			else
			{
				UserLog.addToLog(Constants.ERRORCODES.get("AppMetricsFileNoRead"));
				log.info(Constants.ERRORCODES.get("AppMetricsFileNoRead"));
			}
		} catch (TaskExecFailException e) {
			log.error("Read app metrics file exception", e);
		} catch (Exception e) {
			log.error("Read app metrics file exception", e);
		}

		return false;
	}

	public void disconnectMaster()
	{
		if (sshMaster != null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("ClosedMasterConnection"));
			log.info(Constants.ERRORCODES.get("ClosedMasterConnection"));

			sshMaster.disconnect();
		}
	}

	private boolean startUpSlaves()
	{
		int successRate = 0;

		for (String slaveIP : slaveAddress) {

			//Avoid master IP
			if (slaveIP.equalsIgnoreCase(nodeID))
			{
				successRate++;
				continue;
			}

			ClusterSlave slave = new ClusterSlave();

			if (slave.connectToSlave(slaveIP, JobSession.username, JobSession.password))
			{
				if (slave.transferAndRunScriptFile())
				{
					successRate++;
				}

				slave.disconnectSlave();
			}
		}

		//Only proceed if slave success rate is 100%
		if (successRate == slaveAddress.length)
		{
			return true;
		}

		log.error(Constants.ERRORCODES.get("SlaveFailure"));
		return false;
	}

	private void fetchFromSlaves()
	{
		for (String slaveIP : slaveAddress) {

			//Avoid master IP
			if (slaveIP.equalsIgnoreCase(nodeID))
			{
				continue;
			}

			ClusterSlave slave = new ClusterSlave();

			if (slave.connectToSlave(slaveIP, JobSession.username, JobSession.password))
			{
				slave.readLogFile();
				slave.disconnectSlave();
			}
		}
	}

	private void cleanUpSlaves()
	{
		for (String slaveIP : slaveAddress) {

			//Avoid master IP
			if (slaveIP.equalsIgnoreCase(nodeID))
			{
				continue;
			}

			ClusterSlave slave = new ClusterSlave();

			if (slave.connectToSlave(slaveIP, JobSession.username, JobSession.password))
			{
				slave.killScriptRun();
				slave.cleanUpLogs();
				slave.disconnectSlave();
			}
		}
	}
}
