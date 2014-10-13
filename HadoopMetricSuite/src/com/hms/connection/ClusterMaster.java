package com.hms.connection;

import com.hms.common.Constants;

import net.neoremind.sshxcute.core.ConnBean;
import net.neoremind.sshxcute.core.Result;
import net.neoremind.sshxcute.core.SSHExec;
import net.neoremind.sshxcute.exception.TaskExecFailException;
import net.neoremind.sshxcute.task.CustomTask;
import net.neoremind.sshxcute.task.impl.ExecCommand;

public class ClusterMaster {

	//Private
	SSHExec sshMaster = null;
	
	//Public
	public String[] slaves = null;
	public String errorCode = null;
	
	public boolean connectToMaster(String ipAddress, String username, String password)
	{
		if (!(ipAddress.length() > 0 && username.length() > 0 && password.length() > 0))
		{
			return false;
		}
		
		try {

			ConnBean cb = new ConnBean(ipAddress, username, password);

			sshMaster = SSHExec.getInstance(cb);          

			return sshMaster.connect();

		} catch (Exception e) {

			System.out.println(e.getMessage());
			e.printStackTrace();

			return false;
		} 
	}
	
	public boolean fetchSlaveList()
	{
		if (sshMaster == null)
		{
			errorCode = Constants.NO_MASTER_CONNECTION;
			
			return false;
		}
		
		try {
			CustomTask catCmd = new ExecCommand("cat " + Constants.USER_PATH + Constants.HADOOP_VERSION + Constants.SLAVE_LIST_PATH);

			Result catCmdRes = sshMaster.exec(catCmd);

			if (catCmdRes.isSuccess)
			{
				System.out.println("Return code: " + catCmdRes.rc);
				System.out.println("sysout: " + catCmdRes.sysout);
				
				slaves = catCmdRes.sysout.split("\n");
				
				return true;
			}                        
			else
			{
				System.out.println("Return code: " + catCmdRes.rc);
				System.out.println("error message: " + catCmdRes.error_msg);
			}
		} catch (TaskExecFailException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} 
		
		errorCode = Constants.NO_SLAVE_LIST;

		return false;
	}
	
	public void disconnectMaster()
	{
		if (sshMaster != null)
		{
			sshMaster.disconnect();
		}
	}
}
