package com.hms.connection;

import java.io.PrintWriter;

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

/**
 * @author adithya
 * @version 1.0
 * 
 * This class is responsible for the following:
 * 1) Establish SSH connection with datanode and disconnect after work is done
 * 2) Transfers and runs shell script for collecting platform level metrics on datanode
 * 3) Kills shell script and fetches log files from datanode
 *
 */
public class ClusterSlave {

	private SSHExec sshSlave = null;
	private String nodeID;
	
	private DatabaseManager dbManager = new DatabaseManager();

	static final Logger log = (Logger) LogManager.getLogger(ClusterSlave.class.getName());

	public boolean connectToSlave(String ipAddress, String username, String password)
	{
		if (!(ipAddress.length() > 0 && username.length() > 0 && password.length() > 0))
		{
			UserLog.addToLog(Constants.ERRORCODES.get("WrongSlaveDetails"));
			log.error(Constants.ERRORCODES.get("WrongSlaveDetails"));

			return false;
		}

		try {

			ConnBean cb = new ConnBean(ipAddress, username, password);

			sshSlave = SSHExec.getInstance(cb);      

			if (sshSlave.connect())
			{
				nodeID = ipAddress;
				
				UserLog.addToLog(Constants.ERRORCODES.get("SuccessSlaveConnection") + nodeID);
				log.info(Constants.ERRORCODES.get("SuccessSlaveConnection") + nodeID);

				return true;
			}

			UserLog.addToLog(Constants.ERRORCODES.get("FailedSlaveConnection"));
			log.info(Constants.ERRORCODES.get("FailedSlaveConnection"));

			return false;

		} catch (Exception e) {

			UserLog.addToLog(Constants.ERRORCODES.get("FailedSlaveConnection"));
			log.error("Slave connection exception", e);

			return false;
		} 
	}

	public boolean transferAndRunScriptFile()
	{
		if (sshSlave == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoSlaveConnection"));
			log.error(Constants.ERRORCODES.get("NoSlaveConnection"));
			
			return false;
		}

		try {

			UserLog.addToLog(Constants.ERRORCODES.get("ScriptUpload"));
			log.info(Constants.ERRORCODES.get("ScriptUpload"));

			sshSlave.uploadSingleDataToServer(JobSession.getPathForResource(Constants.SCRIPT_NAME), Constants.USER_PATH + Constants.SCRIPT_NAME);

			CustomTask scriptPermission = new ExecCommand("chmod 755 " + Constants.USER_PATH + Constants.SCRIPT_NAME);
			sshSlave.exec(scriptPermission);

			CustomTask shellMaster = new ExecShellScript(Constants.USER_PATH.substring(0, Constants.USER_PATH.length()-1), "./" + Constants.SCRIPT_NAME  + " " + JobSession.retrievalFrequency + " >> dummy.txt &", "");

			Result resMaster = sshSlave.exec(shellMaster);

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
			log.error("Slave script execution exception", e);
		} catch (Exception e) {
			log.error("Slave script execution exception", e);
		} 
		
		return false;
	}

	public void killScriptRun()
	{
		if (sshSlave == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoSlaveConnection"));
			log.error(Constants.ERRORCODES.get("NoSlaveConnection"));
		}

		try {
			CustomTask killJob = new ExecCommand("pkill " + Constants.SCRIPT_NAME);

			Result res = sshSlave.exec(killJob);

			if (res.isSuccess)
			{
				CustomTask killJob1 = new ExecCommand("pkill vmstat");
				sshSlave.exec(killJob1);
				
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
	
	public void cleanUpLogs()
	{
		if (sshSlave == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoMasterConnection"));
			log.error(Constants.ERRORCODES.get("NoMasterConnection"));
		}

		try {
			CustomTask cleanJob = new ExecCommand("rm hms_*");

			Result res = sshSlave.exec(cleanJob);
			
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

	public void readLogFile()
	{
		if (sshSlave == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoSlaveConnection"));
			log.error(Constants.ERRORCODES.get("NoSlaveConnection"));
		}
		
		killScriptRun();

		UserLog.addToLog(Constants.ERRORCODES.get("AboutToReadLogFile"));
		log.info(Constants.ERRORCODES.get("AboutToReadLogFile"));
		
		if (readLog(Constants.CPU_LOG_NAME) && readLog(Constants.DISK_LOG_NAME) && readLog(Constants.MEM_LOG_NAME) && readLog(Constants.NET_LOG_NAME))
		{
			try
			{
				dbManager.getConnection();
				dbManager.insertIntoPlatformMetrics(nodeID);
			}
			catch (Exception e)
			{
				UserLog.addToLog(Constants.ERRORCODES.get("DBSlaveInsertionError"));
				log.error(Constants.ERRORCODES.get("DBSlaveInsertionError"));
			}
			
			dbManager.closeConnection();
		}
		
		cleanUpLogs();
	}

	private boolean readLog(String logFileName)
	{
		if (sshSlave == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoSlaveConnection"));
			log.error(Constants.ERRORCODES.get("NoSlaveConnection"));
		}

		try {
			CustomTask grep = new ExecCommand("cat " + Constants.USER_PATH + logFileName);

			Result res = sshSlave.exec(grep);

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
	
	public void disconnectSlave()
	{
		if (sshSlave != null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("ClosedSlaveConnection"));
			log.info(Constants.ERRORCODES.get("ClosedSlaveConnection"));

			sshSlave.disconnect();
		}
	}
}
