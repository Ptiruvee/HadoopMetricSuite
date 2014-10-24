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
import com.hms.common.UserLog;

public class ClusterMaster {

	//Private
	SSHExec sshMaster = null;

	//Public
	public String[] slaves = null;
	public String scriptProcessID = null;

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
				UserLog.addToLog(Constants.ERRORCODES.get("SuccessMasterConnection"));
				log.info(Constants.ERRORCODES.get("SuccessMasterConnection"));

				return true;
			}

			UserLog.addToLog(Constants.ERRORCODES.get("FailedMasterConnection"));
			log.info(Constants.ERRORCODES.get("FailedMasterConnection"));

			return false;

		} catch (Exception e) {

			UserLog.addToLog(Constants.ERRORCODES.get("FailedMasterConnection"));
			log.error("Master connection exception");
			log.error(e);

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

				slaves = catCmdRes.sysout.split("\n");

				return true;
			}                        
			else
			{
				UserLog.addToLog(Constants.ERRORCODES.get("FailedMasterSlaveListFetch"));
				log.info(Constants.ERRORCODES.get("FailedMasterSlaveListFetch"));
			}
		} catch (TaskExecFailException e) {
			log.error("Slave list exception");
			log.error(e);
		} catch (Exception e) {
			log.error("Slave list exception");
			log.error(e);
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

			CustomTask shellMaster = new ExecShellScript(Constants.USER_PATH,"./" + Constants.SCRIPT_NAME + " >> dummy.txt &", null);

			Result resMaster = sshMaster.exec(shellMaster);

			if (resMaster.isSuccess)
			{
				UserLog.addToLog(Constants.ERRORCODES.get("ScriptExecutionSuccess"));
				log.info(Constants.ERRORCODES.get("ScriptExecutionSuccess"));

				scriptProcessID = resMaster.sysout;
			}                        
			else
			{
				UserLog.addToLog(Constants.ERRORCODES.get("ScriptExecutionFailure"));
				log.info(Constants.ERRORCODES.get("ScriptExecutionFailure"));
			}
		} catch (TaskExecFailException e) {
			log.error("Master script execution exception");
			log.error(e);
		} catch (Exception e) {
			log.error("Master script execution exception");
			log.error(e);
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
			log.error("Master script execution exception");
			log.error(e);
		}
	}

	public void runApplicationJob(String type)
	{
		if (sshMaster == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoMasterConnection"));
			log.error(Constants.ERRORCODES.get("NoMasterConnection"));
		}

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
			log.error("Job execution exception");
			log.error(e);
		} catch (Exception e) {
			log.error("Job execution exception");
			log.error(e);
		}
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
			log.error("Script termination exception");
			log.error(e);
		} catch (Exception e) {
			log.error("Script termination exception");
			log.error(e);
		}
	}

	public void readLogFile()
	{
		if (sshMaster == null)
		{
			UserLog.addToLog(Constants.ERRORCODES.get("NoMasterConnection"));
			log.error(Constants.ERRORCODES.get("NoMasterConnection"));
		}
		
		killScriptRun();

		if (readCPULog())
		{
			//Insert into database
		}
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

				PrintWriter logOutput = new PrintWriter(Constants.TEMP_LOG_NAME);
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
			log.error("Read log file exception");
			log.error(e);
		} catch (Exception e) {
			log.error("Read log file exception");
			log.error(e);
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
}
