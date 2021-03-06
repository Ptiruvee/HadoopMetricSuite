package com.hms.database;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.TimeZone;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;

import com.hms.common.Constants;
import com.hms.common.JobSession;
import com.hms.common.OldJob;

/**
 * @author pratyushatiruveedhula
 * @version 1.0
 * 
 * This class is responsible to manage all database operations for this application
 * 1) Creates connection with SQLite
 * 2) Inserts into jobconfig table from ClusterMaster
 * 3) Read log files and inserts into platformmetrics table from ClusterMaster/ClusterSlave
 * 4) Gets experiment count for TabbedScreen
 * 5) Gets experiment list for GraphDisplayScreen
 * 6) Reads database and write as tsv files for GraphDisplayScreen
 * 
 */
public class DatabaseManager {
	Connection connection = null;
	
	private ArrayList<String> time = new ArrayList<>();
	private ArrayList<String> cpu = new ArrayList<>();
	private ArrayList<String> disk_read = new ArrayList<>();
	private ArrayList<String> disk_write = new ArrayList<>();
	private ArrayList<String> disk_readtime = new ArrayList<>();
	private ArrayList<String> disk_writetime = new ArrayList<>();
	private ArrayList<String> disk_iotime = new ArrayList<>();
	private ArrayList<String> memory = new ArrayList<>();
	private ArrayList<String> network_sent = new ArrayList<>();
	private ArrayList<String> network_received = new ArrayList<>();

	static final Logger log = (Logger) LogManager.getLogger(DatabaseManager.class.getName());

	/**
	 * @return connection to database
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public Connection getConnection() throws ClassNotFoundException,
	SQLException {
		Class.forName("org.sqlite.JDBC");
		try {
			log.info("DB HMS Path " + JobSession.hmsPath);
			connection = DriverManager
					.getConnection("jdbc:sqlite:" + JobSession.hmsPath + "HadoopMetrics.sqlite");
			connection.setAutoCommit(false);
			return connection;
		} catch (SQLException e) {
			log.error("Database connection exception", e);
			throw e;
		}
	}

	public void closeConnection()
	{
		try
		{
			connection.close();
		}
		catch(Exception e)
		{
			log.error("Database connection close exception", e);
		}
	}

	/**
	 * fetches data from the database
	 * @throws SQLException
	 */
	public void fetchData(String jobID, String type, boolean wantAllRun, int runs) throws SQLException {

		String tempJobid = jobID;
		
		if (wantAllRun)
		{
			tempJobid = jobID.split(Constants.DELIMITER)[0];
		}
		
		File tsvFile = new File(JobSession.getGraphPath() + type + tempJobid + Constants.CPU +".tsv");

		if (tsvFile.exists())
		{
			return;
		}

		int rowCount = 0;
		
		//First check number of runs
		//Then start fetching data from each experiment
		//Add to map with nodeIP_runNumber
		//Then just plot it
		
		if (wantAllRun && runs > 1)
		{
			log.info("Cluster info fetch for all runs display");
			log.info("Runs " + runs);
			log.info("Size " + jobID.split(Constants.DELIMITER).length);
			log.info("Jobid " + jobID);
			
			String tempJobType = jobID.split(Constants.DELIMITER)[2];
			
			ArrayList<String> jobList = new ArrayList<>();
			
			for (int i = 1; i <= runs; i++) {
				jobList.add(tempJobid + Constants.DELIMITER + i + Constants.DELIMITER + tempJobType);
			}
			
			ArrayList<String> time = new ArrayList<>();
			ArrayList<String> cpu = new ArrayList<>();
			ArrayList<String> disk_read = new ArrayList<>();
			ArrayList<String> disk_write = new ArrayList<>();
			ArrayList<String> disk_readtime = new ArrayList<>();
			ArrayList<String> disk_writetime = new ArrayList<>();
			ArrayList<String> disk_iotime = new ArrayList<>();
			ArrayList<String> memory = new ArrayList<>();
			ArrayList<String> network_sent = new ArrayList<>();
			ArrayList<String> network_received = new ArrayList<>();
			
			HashMap<String, HashMap<String, String>> cpuG = new HashMap<>();
			HashMap<String, HashMap<String, String>> disk_readG = new HashMap<>();
			HashMap<String, HashMap<String, String>> disk_writeG = new HashMap<>();
			HashMap<String, HashMap<String, String>> disk_readtimeG = new HashMap<>();
			HashMap<String, HashMap<String, String>> disk_writetimeG = new HashMap<>();
			HashMap<String, HashMap<String, String>> disk_iotimeG = new HashMap<>();
			HashMap<String, HashMap<String, String>> memoryG = new HashMap<>();
			HashMap<String, HashMap<String, String>> network_sentG = new HashMap<>();
			HashMap<String, HashMap<String, String>> network_receivedG = new HashMap<>();
			
			int timeSize = 0;
			
			for (String jobid : jobList) {
				
				try {
					Statement statement = connection.createStatement();
					ResultSet rs = statement
							.executeQuery("SELECT timestamp, avg(CPU), avg(Disk_read), avg(Disk_write), avg(Disk_readtime), avg(Disk_writetime), avg(Disk_iotime), avg(Memory), avg(Network_sent), avg(Network_received) FROM PlatformMetrics where jobid='" + jobID +"' group by timestamp");
					while (rs.next()) {
						time.add( rs.getString("timestamp"));
						cpu.add( String.format(Constants.DECIMAL_LIMIT, rs.getDouble("avg(CPU)")));
						disk_read.add( String.format(Constants.DECIMAL_LIMIT, rs.getDouble("avg(Disk_read)")));
						disk_write.add( String.format(Constants.DECIMAL_LIMIT, rs.getDouble("avg(Disk_write)")));
						disk_readtime.add( String.format(Constants.DECIMAL_LIMIT, rs.getDouble("avg(Disk_readtime)")));
						disk_writetime.add( String.format(Constants.DECIMAL_LIMIT, rs.getDouble("avg(Disk_writetime)")));
						disk_iotime.add( String.format(Constants.DECIMAL_LIMIT, rs.getDouble("avg(Disk_iotime)")));
						memory.add( String.format(Constants.DECIMAL_LIMIT, rs.getDouble("avg(Memory)")));
						network_sent.add( String.format(Constants.DECIMAL_LIMIT, rs.getDouble("avg(Network_sent)")));
						network_received.add( String.format(Constants.DECIMAL_LIMIT, rs.getDouble("avg(Network_received)")));
					}
					
					timeSize = time.size();
					
					cpuG.put(jobid, averageValueToNormalize(cpu, timeSize));
					disk_readG.put(jobid, averageValueToNormalize(disk_read, timeSize));
					disk_writeG.put(jobid, averageValueToNormalize(disk_write, timeSize));
					disk_readtimeG.put(jobid, averageValueToNormalize(disk_readtime, timeSize));
					disk_writetimeG.put(jobid, averageValueToNormalize(disk_writetime, timeSize));
					disk_iotimeG.put(jobid, averageValueToNormalize(disk_iotime, timeSize));
					memoryG.put(jobid, averageValueToNormalize(memory, timeSize));
					network_sentG.put(jobid, averageValueToNormalize(network_sent, timeSize));
					network_receivedG.put(jobid, averageValueToNormalize(network_received, timeSize));
					
					//Clear for next iteration
					time.clear();
					cpu.clear();
					disk_read.clear();
					disk_write.clear();
					disk_readtime.clear();
					disk_writetime.clear();
					disk_iotime.clear();
					memory.clear();
					network_sent.clear();
					network_received.clear();
					
				} catch (SQLException e) {
					// connection close failed.
					log.error("Database fetch exception", e);
					throw e;
				}
			}
			
			log.info("All set to write to file for cluster multiple runs");
			
			int interval = (int)Math.ceil(timeSize/Constants.MAXIMUM_DATA_VIEW);
			int size = (int)Math.ceil(timeSize/interval);
			
			writeFileForEveryNode(JobSession.getGraphPath() + type + tempJobid + Constants.CPU +".tsv", cpuG, interval, size);
			writeFileForEveryNode(JobSession.getGraphPath() + type + tempJobid + Constants.MEMORY +".tsv", memoryG, interval, size);
			writeTwoValueFileForEveryNode(JobSession.getGraphPath() + type + tempJobid + Constants.DISK_RW +".tsv", "Reads", "Writes", disk_readG, disk_writeG, interval, size);
			writeTwoValueFileForEveryNode(JobSession.getGraphPath() + type + tempJobid + Constants.NETWORK +".tsv", "Sent", "Received", network_sentG, network_receivedG, interval, size);
			writeThreeValueFileForEveryNode(JobSession.getGraphPath() + type + tempJobid + Constants.DISK_TIME +".tsv", "Read time", "Write time", "IO Time", disk_readtimeG, disk_writetimeG, disk_iotimeG, interval, size);
			
			return;
			
		}
		
		if (type == Constants.NODE)
		{
			log.info("All node database fetch...");
			
			Statement statement = connection.createStatement();
			
			ArrayList<String> nodeList = new ArrayList<>();
			
			try {
				ResultSet rs = statement
						.executeQuery("SELECT distinct(nodeid) from platformmetrics where jobid='" + jobID + "'");
				
				while (rs.next()) {
					nodeList.add( rs.getString("nodeid"));
				}
			} catch (SQLException e) {
				// connection close failed.
				log.error("All node database fetch exception", e);
				throw e;
			}
			
			//select timestamp, nodeid, cpu from platformmetrics where jobid='4_WC' and nodeid in (select distinct(nodeid) from platformmetrics where jobid='4_WC')
			//For different runs of same exp, average values by query like SELECT timestamp, avg(CPU) platformmetrics where jobid in (13_1_WC, 13_2_WC) and nodeid in (select distinct(nodeid) from platformmetrics where jobid='4_WC')
			//Dictionary of dictionaries
			//nodeid as key contains another dictionary with timestamp as key
			//Make tab based tsv based on the dictionaries
			
			ArrayList<String> time = new ArrayList<>();
			ArrayList<String> cpu = new ArrayList<>();
			ArrayList<String> disk_read = new ArrayList<>();
			ArrayList<String> disk_write = new ArrayList<>();
			ArrayList<String> disk_readtime = new ArrayList<>();
			ArrayList<String> disk_writetime = new ArrayList<>();
			ArrayList<String> disk_iotime = new ArrayList<>();
			ArrayList<String> memory = new ArrayList<>();
			ArrayList<String> network_sent = new ArrayList<>();
			ArrayList<String> network_received = new ArrayList<>();
			
			HashMap<String, HashMap<String, String>> cpuG = new HashMap<>();
			HashMap<String, HashMap<String, String>> disk_readG = new HashMap<>();
			HashMap<String, HashMap<String, String>> disk_writeG = new HashMap<>();
			HashMap<String, HashMap<String, String>> disk_readtimeG = new HashMap<>();
			HashMap<String, HashMap<String, String>> disk_writetimeG = new HashMap<>();
			HashMap<String, HashMap<String, String>> disk_iotimeG = new HashMap<>();
			HashMap<String, HashMap<String, String>> memoryG = new HashMap<>();
			HashMap<String, HashMap<String, String>> network_sentG = new HashMap<>();
			HashMap<String, HashMap<String, String>> network_receivedG = new HashMap<>();
			
			int timeSize = 0;
			
			for (String nodeID : nodeList) {
				try {
					
					ResultSet rs = statement
							.executeQuery("SELECT timestamp, CPU, Disk_read, Disk_write, Disk_readtime, Disk_writetime, Disk_iotime, Memory, Network_sent, Network_received FROM PlatformMetrics where jobid='" + jobID +"' AND nodeid ='" + nodeID + "' group by timestamp");
					while (rs.next()) {
						time.add( rs.getString("timestamp"));
						cpu.add( String.format(Constants.DECIMAL_LIMIT, rs.getDouble("CPU")));
						disk_read.add( String.format(Constants.DECIMAL_LIMIT, rs.getDouble("Disk_read")));
						disk_write.add( String.format(Constants.DECIMAL_LIMIT, rs.getDouble("Disk_write")));
						disk_readtime.add( String.format(Constants.DECIMAL_LIMIT, rs.getDouble("Disk_readtime")));
						disk_writetime.add( String.format(Constants.DECIMAL_LIMIT, rs.getDouble("Disk_writetime")));
						disk_iotime.add( String.format(Constants.DECIMAL_LIMIT, rs.getDouble("Disk_iotime")));
						memory.add( String.format(Constants.DECIMAL_LIMIT, rs.getDouble("Memory")));
						network_sent.add( String.format(Constants.DECIMAL_LIMIT, rs.getDouble("Network_sent")));
						network_received.add( String.format(Constants.DECIMAL_LIMIT, rs.getDouble("Network_received")));
					}
					
					timeSize = time.size();
					
					log.info("Time for each node " + timeSize);
					
					cpuG.put(nodeID, averageValueToNormalize(cpu, timeSize));
					disk_readG.put(nodeID, averageValueToNormalize(disk_read, timeSize));
					disk_writeG.put(nodeID, averageValueToNormalize(disk_write, timeSize));
					disk_readtimeG.put(nodeID, averageValueToNormalize(disk_readtime, timeSize));
					disk_writetimeG.put(nodeID, averageValueToNormalize(disk_writetime, timeSize));
					disk_iotimeG.put(nodeID, averageValueToNormalize(disk_iotime, timeSize));
					memoryG.put(nodeID, averageValueToNormalize(memory, timeSize));
					network_sentG.put(nodeID, averageValueToNormalize(network_sent, timeSize));
					network_receivedG.put(nodeID, averageValueToNormalize(network_received, timeSize));
					
					//Clear for next iteration
					time.clear();
					cpu.clear();
					disk_read.clear();
					disk_write.clear();
					disk_readtime.clear();
					disk_writetime.clear();
					disk_iotime.clear();
					memory.clear();
					network_sent.clear();
					network_received.clear();
					
				} catch (SQLException e) {
					// connection close failed.
					log.error("All node database fetch exception", e);
					throw e;
				}
			}
			
			log.info("All set to write to file");
			
			int interval = (int)Math.ceil(timeSize/Constants.MAXIMUM_DATA_VIEW);
			int size = (int)Math.ceil(timeSize/interval);
			
			log.info("Each node interval " + interval);
			
			writeFileForEveryNode(JobSession.getGraphPath() + type + jobID + Constants.CPU +".tsv", cpuG, interval, size);
			writeFileForEveryNode(JobSession.getGraphPath() + type + jobID + Constants.MEMORY +".tsv", memoryG, interval, size);
			writeTwoValueFileForEveryNode(JobSession.getGraphPath() + type + jobID + Constants.DISK_RW +".tsv", "Reads", "Writes", disk_readG, disk_writeG, interval, size);
			writeTwoValueFileForEveryNode(JobSession.getGraphPath() + type + jobID + Constants.NETWORK +".tsv", "Sent", "Received", network_sentG, network_receivedG, interval, size);
			writeThreeValueFileForEveryNode(JobSession.getGraphPath() + type + jobID + Constants.DISK_TIME +".tsv", "Read time", "Write time", "IO Time", disk_readtimeG, disk_writetimeG, disk_iotimeG, interval, size);
			
			return;
		}
		
		time.clear();
		cpu.clear();
		disk_read.clear();
		disk_write.clear();
		disk_readtime.clear();
		disk_writetime.clear();
		disk_iotime.clear();
		memory.clear();
		network_sent.clear();
		network_received.clear();
		
		//Same logic like maintaining dictionary from above for more runs in cluster

		try {
			Statement statement = connection.createStatement();
			ResultSet rs = statement
					.executeQuery("SELECT timestamp, avg(CPU), avg(Disk_read), avg(Disk_write), avg(Disk_readtime), avg(Disk_writetime), avg(Disk_iotime), avg(Memory), avg(Network_sent), avg(Network_received) FROM PlatformMetrics where jobid='" + jobID +"' group by timestamp");
			while (rs.next()) {
				time.add( rs.getString("timestamp"));
				cpu.add( String.format(Constants.DECIMAL_LIMIT, rs.getDouble("avg(CPU)")));
				disk_read.add( String.format(Constants.DECIMAL_LIMIT, rs.getDouble("avg(Disk_read)")));
				disk_write.add( String.format(Constants.DECIMAL_LIMIT, rs.getDouble("avg(Disk_write)")));
				disk_readtime.add( String.format(Constants.DECIMAL_LIMIT, rs.getDouble("avg(Disk_readtime)")));
				disk_writetime.add( String.format(Constants.DECIMAL_LIMIT, rs.getDouble("avg(Disk_writetime)")));
				disk_iotime.add( String.format(Constants.DECIMAL_LIMIT, rs.getDouble("avg(Disk_iotime)")));
				memory.add( String.format(Constants.DECIMAL_LIMIT, rs.getDouble("avg(Memory)")));
				network_sent.add( String.format(Constants.DECIMAL_LIMIT, rs.getDouble("avg(Network_sent)")));
				network_received.add( String.format(Constants.DECIMAL_LIMIT, rs.getDouble("avg(Network_received)")));

				rowCount++;
			}
		} catch (SQLException e) {
			// connection close failed.
			log.error("Database fetch exception", e);
			throw e;
		}

		log.info("Total fetched rows " + rowCount);
		log.info("Total size " + time.size());
		log.info("Maximum data limit " + Constants.MAXIMUM_DATA_VIEW);
		log.info("Original Interval " + Math.ceil(time.size()/Constants.MAXIMUM_DATA_VIEW));
		log.info("Modified Interval " + (int)Math.ceil(time.size()/Constants.MAXIMUM_DATA_VIEW));

		if (time.size() > Constants.MAXIMUM_DATA_VIEW)
		{
			log.info("************* Going to normalize to show the best fit graph for cluster" + time.size()/Constants.MAXIMUM_DATA_VIEW);

			int size = time.size();
			int interval = (int)Math.ceil(size/Constants.MAXIMUM_DATA_VIEW);
			
			log.info("******** Interval gap " + interval);
			
			writeDBToFile(JobSession.getGraphPath() + type + jobID + Constants.CPU +".tsv", averageValueToNormalizeForCluster(cpu, size), interval);
			writeTwoValuesToFile(JobSession.getGraphPath() + type + jobID + Constants.DISK_RW +".tsv", "Reads", "Writes", averageValueToNormalizeForCluster(disk_read, size), averageValueToNormalizeForCluster(disk_write, size), interval);
			writeThreeValuesToFile(JobSession.getGraphPath() + type + jobID + Constants.DISK_TIME +".tsv", "Read time", "Write time", "IO Time", averageValueToNormalizeForCluster(disk_readtime, size), averageValueToNormalizeForCluster(disk_writetime, size), averageValueToNormalizeForCluster(disk_iotime, size), interval);
			writeDBToFile(JobSession.getGraphPath() + type + jobID + Constants.MEMORY +".tsv", averageValueToNormalizeForCluster(memory, size), interval);
			writeTwoValuesToFile(JobSession.getGraphPath() + type + jobID + Constants.NETWORK +".tsv", "Sent", "Received", averageValueToNormalizeForCluster(network_sent, size), averageValueToNormalizeForCluster(network_received, size), interval);
		}
		else
		{
			log.info("************* Just normal graph for cluster");

			int interval = (int)Math.ceil(time.size()/Constants.MAXIMUM_DATA_VIEW);
			
			log.info("******** Interval gap " + interval);
			
			writeDBToFile(JobSession.getGraphPath() + type + jobID + Constants.CPU +".tsv", cpu, interval);
			writeTwoValuesToFile(JobSession.getGraphPath() + type + jobID + Constants.DISK_RW +".tsv", "Reads", "Writes", disk_read, disk_write, interval);
			writeThreeValuesToFile(JobSession.getGraphPath() + type + jobID + Constants.DISK_TIME +".tsv", "Read time", "Write time", "IO Time", disk_readtime, disk_writetime, disk_iotime, interval);
			writeDBToFile(JobSession.getGraphPath() + type + jobID + Constants.MEMORY +".tsv", memory, interval);
			writeTwoValuesToFile(JobSession.getGraphPath() + type + jobID + Constants.NETWORK +".tsv", "Sent", "Received", network_sent, network_received, interval);
		}
	}

	private HashMap<String, String> averageValueToNormalize(ArrayList<String> valueList, int factor)
	{
		double value = 0;
		
		int count = 0, time = 0;
		int size = factor;
		int interval = (int)Math.ceil(size/Constants.MAXIMUM_DATA_VIEW);
		
		HashMap<String, String> temp = new HashMap<>();

		//To handle zero based calculation
		size--;

		while (size > 0) {
			value += Double.parseDouble(valueList.get(size));
			count++;

			if (count == interval)
			{
				value = value/interval;
				count = 0;

				temp.put(Integer.toString(time++), String.format(Constants.DECIMAL_LIMIT, value));
				
				value = 0;
			}

			size--;
		}

		//Just for the last value
		temp.put(Integer.toString(time++), String.format(Constants.DECIMAL_LIMIT, value/count));
		
		return temp;
	}
	
	private ArrayList<String> averageValueToNormalizeForCluster(ArrayList<String> valueList, int size)
	{
		double value = 0;
		int interval = (int)Math.ceil(size/Constants.MAXIMUM_DATA_VIEW);
		int count = 0;

		ArrayList<String> temp = new ArrayList<>();

		//To handle zero based calculation
		size--;

		//CPU
		while (size > 0) {
			value += Double.parseDouble(valueList.get(size));
			count++;

			if (count == interval)
			{
				value = value/interval;
				count = 0;

				temp.add(String.format(Constants.DECIMAL_LIMIT, value));
				
				value = 0;
			}

			size--;
		}

		//Just for the last value
		temp.add(String.format(Constants.DECIMAL_LIMIT, value/count));
		
		return temp;
	}

	/**
	 * writes a metric to File
	 * @param filename
	 */
	
	private void writeFileForEveryNode(String filename, HashMap<String, HashMap<String, String>> temp, int interval, int timeEnd) {
		
		log.info("File name " + filename);
		
		try {
			FileWriter fileWriter = new FileWriter(filename);
			BufferedWriter writer = new BufferedWriter(fileWriter);
			
			StringBuilder fileString = new StringBuilder();
			fileString.append("date");
			fileString.append("\t");
			
			for (String nodeID : temp.keySet()) {
				fileString.append(nodeID);
				fileString.append("\t");
			}
			
			fileString.append("\n");
			
			writer.write(fileString.toString());
			fileString.setLength(0);
			
			for (int i = 0; i < timeEnd; i++) {
				
				fileString.append("" + i * interval);
				fileString.append("\t");
				
				for (String nodeID : temp.keySet()) {
					fileString.append(temp.get(nodeID).get("" + i));
					fileString.append("\t");
				}
				
				fileString.append("\n");
				writer.write(fileString.toString());
				fileString.setLength(0);
			}
			
			writer.close();
		} catch (IOException e) {
			log.error("Database file writing exception", e);
		}
	}
	
	private void writeTwoValueFileForEveryNode(String filename, String name1, String name2, HashMap<String, HashMap<String, String>> temp1, HashMap<String, HashMap<String, String>> temp2, int interval, int timeEnd) {
		
		log.info("File name " + filename);
		
		try {
			FileWriter fileWriter = new FileWriter(filename);
			BufferedWriter writer = new BufferedWriter(fileWriter);
			
			StringBuilder fileString = new StringBuilder();
			fileString.append("date");
			fileString.append("\t");
			
			for (String nodeID : temp1.keySet()) {
				fileString.append(nodeID + "_" + name1);
				fileString.append("\t");
				fileString.append(nodeID + "_" + name2);
				fileString.append("\t");
			}
			
			fileString.append("\n");
			
			writer.write(fileString.toString());
			fileString.setLength(0);
			
			for (int i = 0; i < timeEnd; i++) {
				
				fileString.append("" + i * interval);
				fileString.append("\t");
				
				for (String nodeID : temp1.keySet()) {
					fileString.append(temp1.get(nodeID).get("" + i));
					fileString.append("\t");
				}
				
				for (String nodeID : temp2.keySet()) {
					fileString.append(temp2.get(nodeID).get("" + i));
					fileString.append("\t");
				}
				
				fileString.append("\n");
				writer.write(fileString.toString());
				fileString.setLength(0);
			}
			
			writer.close();
		} catch (IOException e) {
			log.error("Database file writing exception", e);
		}
	}
	
	private void writeThreeValueFileForEveryNode(String filename, String name1, String name2, String name3, HashMap<String, HashMap<String, String>> temp1, HashMap<String, HashMap<String, String>> temp2, HashMap<String, HashMap<String, String>> temp3, int interval, int timeEnd) {
		
		log.info("File name " + filename);
		
		try {
			FileWriter fileWriter = new FileWriter(filename);
			BufferedWriter writer = new BufferedWriter(fileWriter);
			
			StringBuilder fileString = new StringBuilder();
			fileString.append("date");
			fileString.append("\t");
			
			for (String nodeID : temp1.keySet()) {
				fileString.append(nodeID + "_" + name1);
				fileString.append("\t");
				fileString.append(nodeID + "_" + name2);
				fileString.append("\t");
				fileString.append(nodeID + "_" + name3);
				fileString.append("\t");
			}
			
			fileString.append("\n");
			
			writer.write(fileString.toString());
			fileString.setLength(0);
			
			for (int i = 0; i < timeEnd; i++) {
				
				fileString.append("" + i * interval);
				fileString.append("\t");
				
				for (String nodeID : temp1.keySet()) {
					fileString.append(temp1.get(nodeID).get("" + i));
					fileString.append("\t");
				}
				
				for (String nodeID : temp2.keySet()) {
					fileString.append(temp2.get(nodeID).get("" + i));
					fileString.append("\t");
				}
				
				for (String nodeID : temp3.keySet()) {
					fileString.append(temp3.get(nodeID).get("" + i));
					fileString.append("\t");
				}
				
				fileString.append("\n");
				writer.write(fileString.toString());
				fileString.setLength(0);
			}
			
			writer.close();
		} catch (IOException e) {
			log.error("Database file writing exception", e);
		}
	}
	
	private void writeDBToFile(String filename, ArrayList<String> temp, int interval) {

		try {
			FileWriter fileWriter = new FileWriter(filename);
			BufferedWriter writer = new BufferedWriter(fileWriter);
			writer.write("letter\tfrequency\n");
			for (int i = 0; i < temp.size(); i++) {
				writer.append(interval * i + "\t" + temp.get(i) + "\n");
			}
			writer.close();
		} catch (IOException e) {
			log.error("Database file writing exception", e);
		}
	}

	private void writeTwoValuesToFile(String filename, String title1, String title2, ArrayList<String> temp, ArrayList<String> temp1, int interval) {

		try {
			FileWriter fileWriter = new FileWriter(filename);
			BufferedWriter writer = new BufferedWriter(fileWriter);
			writer.write("date\t" + title1 + "\t" + title2 + "\n");
			for (int i = 0; i < temp.size(); i++) {
				writer.append(interval * i + "\t" + temp.get(i) + "\t" + temp1.get(i) + "\n");
			}
			writer.close();
		} catch (IOException e) {
			log.error("Database file two value writing exception", e);
		}
	}

	private void writeThreeValuesToFile(String filename, String title1, String title2, String title3, ArrayList<String> temp, ArrayList<String> temp1, ArrayList<String> temp2, int interval) {

		try {
			FileWriter fileWriter = new FileWriter(filename);
			BufferedWriter writer = new BufferedWriter(fileWriter);
			writer.write("date\t" + title1 + "\t" + title2 + "\t" + title3 + "\n");
			for (int i = 0; i < temp.size(); i++) {
				writer.append(interval * i + "\t" + temp.get(i) + "\t" + temp1.get(i) + "\t" + temp2.get(i) + "\n");
			}
			writer.close();
		} catch (IOException e) {
			log.error("Database file three value writing exception", e);
		}
	}

	/**
	 * @throws SQLException
	 */
	public void insertIntoJobConfig() throws SQLException {
		try {
			String values = "'" + JobSession.jobID + "','" + JobSession.nodes + "','" + JobSession.datasize +"','" + JobSession.startTime +"','" + JobSession.endTime +"','" + JobSession.expectedRuns +"'";
			Statement stmt = connection.createStatement();
			stmt.executeUpdate("INSERT INTO JobConfig (JobId, Nodes, DataSize, StartTime, EndTime, Runs) VALUES(" + values +")");
			connection.commit();
		} catch (SQLException e) {
			log.error("Database job config insertion exception", e);
			throw e;
		}
	}

	/**
	 * @throws IOException
	 */
	public void insertIntoPlatformMetrics(String nodeID) throws IOException {

		readLogFile(nodeID + Constants.TEMP_LOG_NAME + Constants.CPU_LOG_NAME, Constants.CPU);
		readLogFile(nodeID + Constants.TEMP_LOG_NAME + Constants.DISK_LOG_NAME, Constants.DISK);
		readLogFile(nodeID + Constants.TEMP_LOG_NAME + Constants.MEM_LOG_NAME, Constants.MEMORY);
		readLogFile(nodeID + Constants.TEMP_LOG_NAME + Constants.NET_LOG_NAME, Constants.NETWORK);

		DateFormat gmtFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");
		gmtFormat.setTimeZone(TimeZone.getTimeZone("EST"));

		//date obtained here is in IST on my system which needs to be converted into GMT.
		Date stime = new Date(Long.valueOf(JobSession.startTime) * 1000);
		Date etime = new Date(Long.valueOf(JobSession.endTime) * 1000);

		String start = gmtFormat.format(stime);
		String end = gmtFormat.format(etime);
		log.info("Start time " + start);
		log.info("End time " + end);

		long difference = etime.getTime() - stime.getTime(); 

		log.info("Diff " + difference);
		log.info("Diff 1 " + difference/1000);

		//Compute time
		long runningInterval = difference/1000; 

		time.clear();

		for (int i = 0; i < runningInterval; i++) {
			time.add("" + i);
		}

		log.info("Before Time size " + time.size());
		log.info("Before CPU size " + cpu.size());
		log.info("Before Disk_read size " + disk_read.size());
		log.info("Before Disk_write size " + disk_write.size());
		log.info("Before Disk_readtime size " + disk_readtime.size());
		log.info("Before Disk_writetime size " + disk_writetime.size());
		log.info("Before Disk_iotime size " + disk_iotime.size());
		log.info("Before Memory size " + memory.size());
		log.info("Before Network_sent size " + network_sent.size());
		log.info("Before Network_received size " + network_received.size());

		log.info("Gap " + (runningInterval - cpu.size()));
		
		//Add extra data if required
		if (cpu.size() < runningInterval)
		{
			long diff = runningInterval - cpu.size();
			for (int i = 0; i < diff; i++) {
				cpu.add("0");
			}
		}

		if (disk_read.size() < runningInterval)
		{
			long diff = runningInterval - disk_read.size();
			for (int i = 0; i < diff; i++) {
				disk_read.add("0");
			}
		}

		if (disk_write.size() < runningInterval)
		{
			long diff = runningInterval - disk_write.size();
			for (int i = 0; i < diff; i++) {
				disk_write.add("0");
			}
		}

		if (disk_readtime.size() < runningInterval)
		{
			long diff = runningInterval - disk_readtime.size();
			for (int i = 0; i < diff; i++) {
				disk_readtime.add("0");
			}
		}

		if (disk_writetime.size() < runningInterval)
		{
			long diff = runningInterval - disk_writetime.size();
			for (int i = 0; i < diff; i++) {
				disk_writetime.add("0");
			}
		}

		if (disk_iotime.size() < runningInterval)
		{
			long diff = runningInterval - disk_iotime.size();
			for (int i = 0; i < diff; i++) {
				disk_iotime.add("0");
			}
		}

		if (memory.size() < runningInterval)
		{
			long diff = runningInterval - memory.size();
			for (int i = 0; i < diff; i++) {
				memory.add("0");
			}
		}

		if (network_sent.size() < runningInterval)
		{
			long diff = runningInterval - network_sent.size();
			for (int i = 0; i < diff; i++) {
				network_sent.add("0");
			}
		}

		if (network_received.size() < runningInterval)
		{
			long diff = runningInterval - network_received.size();
			for (int i = 0; i < diff; i++) {
				network_received.add("0");
			}
		}

		log.info("After Time size " + time.size());
		log.info("After CPU size " + cpu.size());
		log.info("After Disk_read size " + disk_read.size());
		log.info("After Disk_write size " + disk_write.size());
		log.info("After Disk_readtime size " + disk_readtime.size());
		log.info("After Disk_writetime size " + disk_writetime.size());
		log.info("After Disk_iotime size " + disk_iotime.size());
		log.info("After Memory size " + memory.size());
		log.info("After Network_sent size " + network_sent.size());
		log.info("After Network_received size " + network_received.size());

		try {
			String query = null;
			Statement stmt= connection.createStatement();

			for (int i = 0; i < time.size(); i++) {
				query = "INSERT INTO PlatformMetrics VALUES ("
						+ time.get(i) + ","
						+ "'" + JobSession.jobID + "',"
						+ cpu.get(i) + ","
						+ memory.get(i) + ","
						+ network_sent.get(i) + ","
						+ disk_read.get(i) + ","
						+ "'" + nodeID + "',"
						+ disk_write.get(i) + ","
						+ disk_readtime.get(i) + ","
						+ disk_writetime.get(i) + ","
						+ disk_iotime.get(i) + ","
						+ network_received.get(i) + ")";
				stmt.addBatch(query);
			}
			stmt.executeBatch();
			connection.commit();
		} catch (SQLException e) {
			log.error("Database platform metrics insertion exception", e);
		} 
	}

	private void readLogFile(String logFileName, String type)
	{
		if (type.equalsIgnoreCase(Constants.CPU))
		{
			readCPULogFile(logFileName);
			return;
		}
		
		ArrayList<String> temp = new ArrayList<>();
		ArrayList<String> temp1 = new ArrayList<>();
		ArrayList<String> temp2 = new ArrayList<>();
		ArrayList<String> temp3 = new ArrayList<>();
		ArrayList<String> temp4 = new ArrayList<>();

		try
		{
			BufferedReader bufferReadForFile = new BufferedReader(
					new FileReader(logFileName));
			try {

				String line = bufferReadForFile.readLine();
				String[] lineContent;
				
				while (line != null) {
					if (line.length() == 0) {
						line = bufferReadForFile.readLine();
						continue;
					}

					lineContent = line.split(" ");

					if (lineContent.length < 2)
					{
						line = bufferReadForFile.readLine();
						continue;
					}

					try
					{
						if (Long.parseLong(lineContent[0]) >= Long.parseLong(JobSession.startTime) && Long.parseLong(lineContent[0]) <= Long.parseLong(JobSession.endTime))
						{
							temp.add(lineContent[1]);

							if (type.equalsIgnoreCase(Constants.DISK))
							{
								temp1.add(lineContent[2]);
								temp2.add(lineContent[3]);
								temp3.add(lineContent[4]);
								temp4.add(lineContent[5]);
							}
							else if (type.equalsIgnoreCase(Constants.NETWORK))
							{
								temp1.add(lineContent[2]);
							}
						}
					}
					catch (Exception e)
					{
						log.error("Here is the long input" + lineContent[0] + "*");
						log.error("Here is the long input" + lineContent[1] + "*");
						log.error("Start" + JobSession.startTime + "*");
						log.error("End" + JobSession.endTime + "*");
						log.error("Long format exception", e);
					}

					line = bufferReadForFile.readLine();
				}

				if (type.equalsIgnoreCase(Constants.DISK))
				{
					disk_read = temp;
					disk_write = temp1;
					disk_readtime = temp2;
					disk_writetime = temp3;
					disk_iotime = temp4;
				}
				else if (type.equalsIgnoreCase(Constants.MEMORY))
				{
					memory = temp;
				}
				else if (type.equalsIgnoreCase(Constants.NETWORK))
				{
					network_received = temp;
					network_sent = temp1;
				}
			} catch (Exception e) {
				log.error("Database log content exception", e);
			}
			finally {
				bufferReadForFile.close();
			}
		}
		catch (Exception e)
		{
			log.error("Database log file read exception", e);
		}
	} 
	
	private void readCPULogFile(String logFileName)
	{
		log.info("******************* CPU ******************");
		log.info("CPU filepath " + logFileName);
		
		ArrayList<String> temp = new ArrayList<>();
		
		Date stime = new Date(Long.valueOf(JobSession.startTime) * 1000);
		Date etime = new Date(Long.valueOf(JobSession.endTime) * 1000);
		
		long difference = (etime.getTime() - stime.getTime())/1000; 
		
		DateFormat gmtFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");
		gmtFormat.setTimeZone(TimeZone.getTimeZone("EST"));

		String start = gmtFormat.format(stime);
		String end = gmtFormat.format(etime);
		log.error("Start time " + start);
		log.error("End time " + end);
		
		log.info("Difference " + difference);
		
		try
		{
			BufferedReader bufferReadForFile = new BufferedReader(
					new FileReader(logFileName));
			try {

				String line = bufferReadForFile.readLine();
				String[] lineContent;
				
				log.info("Line " + line);
				log.info("Inside date " + gmtFormat.format(new Date(Long.valueOf(line) * 1000)));
				
				long startingDiff = 0;
				
				if (line.length() > 0)
				{
					Date starttime = new Date(Long.parseLong(line) * 1000);
					startingDiff = (stime.getTime() - starttime.getTime())/1000;
				}
				
				log.info("Staring diff " + startingDiff);
				
				//To skip second, third line and start reading from fourth line
				line = bufferReadForFile.readLine();
				line = bufferReadForFile.readLine();
				line = bufferReadForFile.readLine();
				
				int lineContentLen = 0;
				
				double util = 0;
				
				while (line != null) {
					
					if (difference == 0)
					{
						break;
					}
					
					if (line.length() == 0) {
						line = bufferReadForFile.readLine();
						continue;
					}
					
					if (startingDiff > 0)
					{
						line = bufferReadForFile.readLine();
						startingDiff--;
						continue;
					}

					lineContent = line.split(" ");
					lineContentLen = lineContent.length;
					
					log.info("Line " + line);
					log.info("LineContent length " + lineContentLen);
					
					if (lineContentLen > 5)
					{
						try
						{
							util = (100 - Integer.parseInt(lineContent[lineContentLen-5]))/100.0;
							
							temp.add("" + util);
							log.info("LineContent idle " + lineContent[lineContentLen-5]);
							log.info("LineContent util " + util);
						}
						catch (Exception e)
						{
							log.error("Excpetion when trying to compute CPU utilization");
						}
					}

					line = bufferReadForFile.readLine();
					
					difference--;
				}
				
				log.info("CPU utilization done " + temp.size());
				cpu = temp;

			} catch (Exception e) {
				log.error("CPU log content exception", e);
			}
			finally {
				bufferReadForFile.close();
			}
		}
		catch (Exception e)
		{
			log.error("Database log file read exception", e);
		}
	} 

	public int getExperimentCount()
	{
		int count = 0;

		try {
			Statement statement = connection.createStatement();
			
			//Single run experiment
			ResultSet rs = statement
					.executeQuery("Select count(jobid) from JobConfig where runs=1");
			while (rs.next()) {
				count = Integer.parseInt(rs.getString("count(jobid)"));
			}
			
			rs = statement
					.executeQuery("Select count(jobid) from JobConfig where runs=2");
			while (rs.next()) {
				count += Integer.parseInt(rs.getString("count(jobid)"))/2;
			}
			
			rs = statement
					.executeQuery("Select count(jobid) from JobConfig where runs=3");
			while (rs.next()) {
				count += Integer.parseInt(rs.getString("count(jobid)"))/3;
			}
		} catch (SQLException e) {
			// connection close failed.
			log.error("Database fetch exception", e);
		}

		return count;
	}

	public ArrayList<OldJob> getOldJobs()
	{
		ArrayList<OldJob> temp = new ArrayList<>();

		Calendar mydate = Calendar.getInstance();

		try {
			Statement statement = connection.createStatement();
			ResultSet rs = statement
					.executeQuery("Select runs, jobid, starttime from JobConfig");
			while (rs.next()) {

				mydate.setTimeInMillis(Long.parseLong(rs.getString("starttime"))*1000);

				OldJob job = new OldJob();
				job.runs = rs.getInt("runs");
				job.jobid = rs.getString("jobid");
				job.startTime = "" + mydate.get(Calendar.MONTH) + "/" + mydate.get(Calendar.DAY_OF_MONTH) + "/" + mydate.get(Calendar.YEAR); 

				temp.add(job);
			}
		} catch (SQLException e) {
			// connection close failed.
			log.error("Database fetch exception", e);
		}

		return temp;
	}
	
	public int getRunNumberForJob(String jobid)
	{
		int runs = 1;
		
		try {
			Statement statement = connection.createStatement();
			ResultSet rs = statement
					.executeQuery("select runs from jobconfig where jobid='" + jobid + "'");
			while (rs.next()) {

				runs = rs.getInt("runs");

			}
		} catch (SQLException e) {
			// connection close failed.
			log.error("Database number of runs exception", e);
		}

		return runs;
	}
}
