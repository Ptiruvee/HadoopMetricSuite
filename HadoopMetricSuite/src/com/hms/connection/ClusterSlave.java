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
				
				UserLog.addToLog(Constants.ERRORCODES.get("SuccessSlaveConnection"));
				log.info(Constants.ERRORCODES.get("SuccessSlaveConnection"));

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

	public void transferAndRunScriptFile()
	{
		if (sshSlave == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoSlaveConnection"));
			log.error(Constants.ERRORCODES.get("NoSlaveConnection"));
		}

		try {

			UserLog.addToLog(Constants.ERRORCODES.get("ScriptUpload"));
			log.info(Constants.ERRORCODES.get("ScriptUpload"));

			sshSlave.uploadSingleDataToServer(Constants.SCRIPT_PATH + Constants.SCRIPT_NAME, Constants.USER_PATH + Constants.SCRIPT_NAME);

			CustomTask scriptPermission = new ExecCommand("chmod 755 " + Constants.USER_PATH + Constants.SCRIPT_NAME);
			sshSlave.exec(scriptPermission);

			CustomTask shellMaster = new ExecShellScript(Constants.USER_PATH.substring(0, Constants.USER_PATH.length()-1), "./" + Constants.SCRIPT_NAME + " >> dummy.txt &", "" + JobSession.retrievalFrequency);

			Result resMaster = sshSlave.exec(shellMaster);

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
			log.error("Slave script execution exception", e);
		} catch (Exception e) {
			log.error("Slave script execution exception", e);
		} 
	}
	
	private void fetchScriptProcessID()
	{
		if (sshSlave == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoSlaveConnection"));
			log.error(Constants.ERRORCODES.get("NoSlaveConnection"));
		}

		try {

			UserLog.addToLog(Constants.ERRORCODES.get("ScriptProcessIDFetch"));
			log.info(Constants.ERRORCODES.get("ScriptProcessIDFetch"));

			CustomTask shellMaster = new ExecShellScript(Constants.USER_PATH.substring(0, Constants.USER_PATH.length()-1), "pgrep " + Constants.SCRIPT_NAME, "");

			Result resMaster = sshSlave.exec(shellMaster);

			if (resMaster.isSuccess)
			{
				UserLog.addToLog(Constants.ERRORCODES.get("ScriptProcessIDFetchSuccess"));
				log.info(Constants.ERRORCODES.get("ScriptProcessIDFetchSuccess"));
				
				JobSession.processIDOfSlaves.put(nodeID, resMaster.sysout);
			}                        
			else
			{
				UserLog.addToLog(Constants.ERRORCODES.get("ScriptProcessIDFetchFailure"));
				log.info(Constants.ERRORCODES.get("ScriptProcessIDFetchFailure"));
			}
		} catch (TaskExecFailException e) {
			log.error("Slave script execution exception", e);
		} catch (Exception e) {
			log.error("Slave script execution exception", e);
		} 
	}

	private void killScriptRun()
	{
		if (sshSlave == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoSlaveConnection"));
			log.error(Constants.ERRORCODES.get("NoSlaveConnection"));
		}

		try {
			CustomTask killJob = new ExecCommand("kill -9 " + JobSession.processIDOfSlaves.get(nodeID));

			Result res = sshSlave.exec(killJob);

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

	public void readLogFile()
	{
		if (sshSlave == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoSlaveConnection"));
			log.error(Constants.ERRORCODES.get("NoSlaveConnection"));
		}
		
		killScriptRun();

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
				UserLog.addToLog(Constants.ERRORCODES.get("LogFileRead"));
				log.info(Constants.ERRORCODES.get("LogFileRead"));

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
