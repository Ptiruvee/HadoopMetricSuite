package com.hms.connection;

import net.neoremind.sshxcute.core.ConnBean;
import net.neoremind.sshxcute.core.Result;
import net.neoremind.sshxcute.core.SSHExec;
import net.neoremind.sshxcute.exception.TaskExecFailException;
import net.neoremind.sshxcute.task.CustomTask;
import net.neoremind.sshxcute.task.impl.ExecCommand;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;

import com.hms.common.Constants;
import com.hms.common.UserLog;

public class ClusterMaster {

	//Private
	SSHExec sshMaster = null;
	
	//Public
	public String[] slaves = null;
	
	static final Logger log = (Logger) LogManager.getLogger(ClusterMaster.class.getName());
	
	public boolean connectToMaster(String ipAddress, String username, String password)
	{
		if (!(ipAddress.length() > 0 && username.length() > 0 && password.length() > 0))
		{
			UserLog.addToLog(Constants.errorCodes.get("WrongMasterDetails"));
			log.error(Constants.errorCodes.get("WrongMasterDetails"));
			
			return false;
		}
		
		try {

			ConnBean cb = new ConnBean(ipAddress, username, password);

			sshMaster = SSHExec.getInstance(cb);      
			
			if (sshMaster.connect())
			{
				UserLog.addToLog(Constants.errorCodes.get("SuccessMasterConnection"));
				log.info(Constants.errorCodes.get("SuccessMasterConnection"));
				
				return true;
			}
			
			UserLog.addToLog(Constants.errorCodes.get("FailedMasterConnection"));
			log.info(Constants.errorCodes.get("FailedMasterConnection"));

			return false;

		} catch (Exception e) {

			UserLog.addToLog(Constants.errorCodes.get("FailedMasterConnection"));
			log.error("Master connection exception");
			log.error(e);
			
			return false;
		} 
	}
	
	public boolean fetchSlaveList()
	{
		if (sshMaster == null)
		{
			UserLog.addToLog(Constants.errorCodes.get("NoMasterConnection"));
			log.error(Constants.errorCodes.get("NoMasterConnection"));
			
			return false;
		}
		
		try {
			CustomTask catCmd = new ExecCommand("cat " + Constants.USER_PATH + Constants.HADOOP_VERSION + Constants.SLAVE_LIST_PATH);

			Result catCmdRes = sshMaster.exec(catCmd);

			if (catCmdRes.isSuccess)
			{
				UserLog.addToLog(Constants.errorCodes.get("SuccessMasterSlaveListFetch"));
				log.info(Constants.errorCodes.get("SuccessMasterSlaveListFetch"));
				
				slaves = catCmdRes.sysout.split("\n");
				
				return true;
			}                        
			else
			{
				UserLog.addToLog(Constants.errorCodes.get("FailedMasterSlaveListFetch"));
				log.info(Constants.errorCodes.get("FailedMasterSlaveListFetch"));
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
	
	public void disconnectMaster()
	{
		if (sshMaster != null)
		{
			UserLog.addToLog(Constants.errorCodes.get("ClosedMasterConnection"));
			log.info(Constants.errorCodes.get("ClosedMasterConnection"));
			
			sshMaster.disconnect();
		}
	}
}
