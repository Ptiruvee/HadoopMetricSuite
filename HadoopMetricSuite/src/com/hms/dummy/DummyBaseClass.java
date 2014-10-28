package com.hms.dummy;

import com.hms.common.Constants;

import com.hms.common.JobSession;
import com.hms.common.UserLog;
import com.hms.connection.ClusterMaster;

public class DummyBaseClass {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		//This is where from which all of us can invoke our code and test
		//Modify and use this class, but never commit!
		
		JobSession.username = "ec2-user";
		JobSession.password = "";
		
		ClusterMaster master = new ClusterMaster();
		
		if (master.connectToMaster("", JobSession.username, JobSession.password))
		{
			master.fetchSlaveList();
			master.transferAndRunScriptFile();
			master.runApplicationJob(Constants.WORD_COUNT);
			master.disconnectMaster();
		}
		
		System.out.println("Log message" + UserLog.getUserLog());
	}

}
