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
	private String scriptProcessID = null;
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
						
				UserLog.addToLog(Constants.ERRORCODES.get("SuccessMasterConnection"));
				log.info(Constants.ERRORCODES.get("SuccessMasterConnection"));

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

	public void transferAndRunScriptFile()
	{
		if (sshMaster == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoMasterConnection"));
			log.error(Constants.ERRORCODES.get("NoMasterConnection"));
		}

		try {

			UserLog.addToLog(Constants.ERRORCODES.get("ScriptUpload"));
			log.info(Constants.ERRORCODES.get("ScriptUpload"));

			sshMaster.uploadSingleDataToServer(Constants.SCRIPT_PATH + Constants.SCRIPT_NAME, Constants.USER_PATH + Constants.SCRIPT_NAME);

			CustomTask scriptPermission = new ExecCommand("chmod 755 " + Constants.USER_PATH + Constants.SCRIPT_NAME);
			sshMaster.exec(scriptPermission);

			CustomTask shellMaster = new ExecShellScript(Constants.USER_PATH.substring(0, Constants.USER_PATH.length()-1), "./" + Constants.SCRIPT_NAME + " >> dummy.txt &", "");

			Result resMaster = sshMaster.exec(shellMaster);

			if (resMaster.isSuccess)
			{
				UserLog.addToLog(Constants.ERRORCODES.get("ScriptExecutionSuccess"));
				log.info(Constants.ERRORCODES.get("ScriptExecutionSuccess"));
				
				fetchScriptProcessID();
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
	}
	
	private void fetchScriptProcessID()
	{
		if (sshMaster == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoMasterConnection"));
			log.error(Constants.ERRORCODES.get("NoMasterConnection"));
		}

		try {

			UserLog.addToLog(Constants.ERRORCODES.get("ScriptProcessIDFetch"));
			log.info(Constants.ERRORCODES.get("ScriptProcessIDFetch"));

			CustomTask shellMaster = new ExecShellScript(Constants.USER_PATH.substring(0, Constants.USER_PATH.length()-1), "pgrep " + Constants.SCRIPT_NAME, "");

			Result resMaster = sshMaster.exec(shellMaster);

			if (resMaster.isSuccess)
			{
				UserLog.addToLog(Constants.ERRORCODES.get("ScriptProcessIDFetchSuccess"));
				log.info(Constants.ERRORCODES.get("ScriptProcessIDFetchSuccess"));
				
				scriptProcessID = resMaster.sysout;
			}                        
			else
			{
				UserLog.addToLog(Constants.ERRORCODES.get("ScriptProcessIDFetchFailure"));
				log.info(Constants.ERRORCODES.get("ScriptProcessIDFetchFailure"));
			}
		} catch (TaskExecFailException e) {
			log.error("Master script execution exception", e);
		} catch (Exception e) {
			log.error("Master script execution exception", e);
		} 
	}

	public void transferApplicationJARFile(String type)
	{
		if (sshMaster == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoMasterConnection"));
			log.error(Constants.ERRORCODES.get("NoMasterConnection"));
		}

		try
		{
			sshMaster.uploadSingleDataToServer(Constants.JAR_PATH + Constants.APPLICATIONTYPES.get(type), Constants.USER_PATH + Constants.APPLICATIONTYPES.get(type));

			UserLog.addToLog(Constants.ERRORCODES.get("JarTransferSuccess"));
			log.info(Constants.ERRORCODES.get("JarTransferSuccess"));
		}
		catch (Exception e)
		{
			log.error("Master script execution exception", e);
		}
	}

	public void runApplicationJob(String type)
	{
		if (sshMaster == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoMasterConnection"));
			log.error(Constants.ERRORCODES.get("NoMasterConnection"));
		}
		
		startUpSlaves();
		
		JobSession.startTime = fetchTime();

		try {
			String hadoopPath = Constants.USER_PATH + Constants.HADOOP_VERSION + Constants.HADOOP_BIN + " jar ";
			String applicationPath = Constants.USER_PATH + Constants.APPLICATIONTYPES.get(type);

			CustomTask runJob = new ExecCommand(hadoopPath + applicationPath);
			
			UserLog.addToLog(Constants.ERRORCODES.get("JobAboutToRun"));
			log.info(Constants.ERRORCODES.get("JobAboutToRun"));

			Result res = sshMaster.exec(runJob);

			if (res.isSuccess)
			{
				//Job started running
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
		
		JobSession.endTime = fetchTime();
		
		readLogFile();
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
			CustomTask killJob = new ExecCommand("kill -9 " + scriptProcessID);

			Result res = sshMaster.exec(killJob);

			if (res.isSuccess)
			{
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

	private void readLogFile()
	{
		if (sshMaster == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoMasterConnection"));
			log.error(Constants.ERRORCODES.get("NoMasterConnection"));
		}
		
		killScriptRun();
		
		JobSession.jobID = "" + Math.random();
		JobSession.nodes = slaveAddress.length;
		JobSession.datasize = 100;

		if (readCPULog())
		{
			try
			{
				dbManager.getConnection();
				dbManager.insertIntoJobConfig();
				dbManager.insertIntoPlatformMetrics(nodeID, nodeID + Constants.TEMP_LOG_NAME);
			}
			catch (Exception e)
			{
				UserLog.addToLog(Constants.ERRORCODES.get("DBMasterInsertionError"));
				log.error(Constants.ERRORCODES.get("DBMasterInsertionError"));
			}
			
			dbManager.closeConnection();
		}
		
		fetchFromSlaves();
	}

	private boolean readCPULog()
	{
		if (sshMaster == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoMasterConnection"));
			log.error(Constants.ERRORCODES.get("NoMasterConnection"));
		}

		try {
			CustomTask grep = new ExecCommand("cat " + Constants.USER_PATH + Constants.CPU_LOG_NAME);

			Result res = sshMaster.exec(grep);

			if (res.isSuccess)
			{
				UserLog.addToLog(Constants.ERRORCODES.get("LogFileRead"));
				log.info(Constants.ERRORCODES.get("LogFileRead"));

				PrintWriter logOutput = new PrintWriter(nodeID + Constants.TEMP_LOG_NAME);
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

	public void disconnectMaster()
	{
		if (sshMaster != null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("ClosedMasterConnection"));
			log.info(Constants.ERRORCODES.get("ClosedMasterConnection"));

			sshMaster.disconnect();
		}
	}
	
	private void startUpSlaves()
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
				slave.transferAndRunScriptFile();
				slave.disconnectSlave();
			}
		}
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
}
