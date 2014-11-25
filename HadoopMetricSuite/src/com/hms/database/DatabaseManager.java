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
import java.util.TimeZone;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;

import com.hms.common.Constants;
import com.hms.common.JobSession;
import com.hms.common.OldJob;

/**
 * @author pratyushatiruveedhula
 * 
 */
public class DatabaseManager {
	Connection connection = null;
	private static ArrayList<String> time = new ArrayList<>();
	private static ArrayList<String> cpu = new ArrayList<>();
	private static ArrayList<String> disk_read = new ArrayList<>();
	private static ArrayList<String> disk_write = new ArrayList<>();
	private static ArrayList<String> disk_readtime = new ArrayList<>();
	private static ArrayList<String> disk_writetime = new ArrayList<>();
	private static ArrayList<String> disk_iotime = new ArrayList<>();
	private static ArrayList<String> memory = new ArrayList<>();
	private static ArrayList<String> network_sent = new ArrayList<>();
	private static ArrayList<String> network_received = new ArrayList<>();

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
			connection = DriverManager
					.getConnection("jdbc:sqlite:dat/HadoopMetrics.sqlite");
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
	public void fetchData(String jobID) throws SQLException {

		File tsvFile = new File(Constants.GRAPH_DATA_PATH + jobID + Constants.CPU +".tsv");

		if (tsvFile.exists())
		{
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

		int rowCount = 0;

		try {
			Statement statement = connection.createStatement();
			ResultSet rs = statement
					.executeQuery("SELECT timestamp, avg(CPU), avg(Disk_read), avg(Disk_write), avg(Disk_readtime), avg(Disk_writetime), avg(Disk_iotime), avg(Memory), avg(Network_sent), avg(Network_received) FROM PlatformMetrics where jobid='" + jobID +"' group by timestamp");
			while (rs.next()) {
				time.add( rs.getString("timestamp"));
				cpu.add( String.format("%.2f", rs.getDouble("avg(CPU)")));
				disk_read.add( String.format("%.2f", rs.getDouble("avg(Disk_read)")));
				disk_write.add( String.format("%.2f", rs.getDouble("avg(Disk_write)")));
				disk_readtime.add( String.format("%.2f", rs.getDouble("avg(Disk_readtime)")));
				disk_writetime.add( String.format("%.2f", rs.getDouble("avg(Disk_writetime)")));
				disk_iotime.add( String.format("%.2f", rs.getDouble("avg(Disk_iotime)")));
				memory.add( String.format("%.2f", rs.getDouble("avg(Memory)")));
				network_sent.add( String.format("%.2f", rs.getDouble("avg(Network_sent)")));
				network_received.add( String.format("%.2f", rs.getDouble("avg(Network_received)")));

				rowCount++;
			}
		} catch (SQLException e) {
			// connection close failed.
			log.error("Database fetch exception", e);
			throw e;
		}

		System.out.println("Total fetched rows " + rowCount);
		System.out.println("Total size " + time.size());
		System.out.println("Maximum data limit " + Constants.MAXIMUM_DATA_VIEW);
		System.out.println("Original Interval " + Math.ceil(time.size()/Constants.MAXIMUM_DATA_VIEW));
		System.out.println("Modified Interval " + (int)Math.ceil(time.size()/Constants.MAXIMUM_DATA_VIEW));

		if (time.size() > Constants.MAXIMUM_DATA_VIEW)
		{
			System.out.println("************* Going to normalize to show the best fit graph " + time.size()/Constants.MAXIMUM_DATA_VIEW);

			averageValueToNormalize(jobID);
		}
		else
		{
			System.out.println("************* Just normal graph ");

			writeDBToFile(Constants.GRAPH_DATA_PATH + jobID + Constants.CPU +".tsv", cpu);
			writeTwoValuesToFile(Constants.GRAPH_DATA_PATH + jobID + Constants.DISK_RW +".tsv", "Reads", "Writes", disk_read, disk_write);
			writeThreeValuesToFile(Constants.GRAPH_DATA_PATH + jobID + Constants.DISK_TIME +".tsv", "Read time", "Write time", "IO Time", disk_readtime, disk_writetime, disk_iotime);
			writeDBToFile(Constants.GRAPH_DATA_PATH + jobID + Constants.MEMORY +".tsv", memory);
			writeTwoValuesToFile(Constants.GRAPH_DATA_PATH + jobID + Constants.NETWORK +".tsv", "Sent", "Received", network_sent, network_received);
		}
	}

	private void averageValueToNormalize(String jobID)
	{
		int size = time.size();
		double value = 0;
		int interval = (int)Math.ceil(size/Constants.MAXIMUM_DATA_VIEW);
		int count = 0;

		ArrayList<String> temp = new ArrayList<>();

		ArrayList<String> temp_disk_read = new ArrayList<>();
		ArrayList<String> temp_disk_write = new ArrayList<>();
		ArrayList<String> temp_disk_readtime = new ArrayList<>();
		ArrayList<String> temp_disk_writetime = new ArrayList<>();
		ArrayList<String> temp_disk_iotime = new ArrayList<>();
		ArrayList<String> temp_network_sent = new ArrayList<>();
		
		System.out.println("******** Interval gap " + interval);

		//To handle zero based calculation
		size--;

		//CPU
		while (size > 0) {
			value += Double.parseDouble(cpu.get(size));
			count++;

			if (count == interval)
			{
				value = value/interval;
				count = 0;

				temp.add(String.format("%.2f", value));
				
				value = 0;
			}

			size--;
		}

		System.out.println("CPU temp size " + temp.size());

		writeDBToFile(Constants.GRAPH_DATA_PATH + jobID + Constants.CPU +".tsv", temp);

		temp.clear();
		value = 0;
		count = 0;
		size = time.size() - 1;

		//Disk_read
		while (size > 0) {
			value += Double.parseDouble(disk_read.get(size));
			count++;

			if (count == interval)
			{
				value = value/interval;
				count = 0;

				temp_disk_read.add(String.format("%.2f", value));
				
				value = 0;
			}

			size--;
		}

		temp.clear();
		value = 0;
		count = 0;
		size = time.size() - 1;

		//Disk_write
		while (size > 0) {
			value += Double.parseDouble(disk_write.get(size));
			count++;

			if (count == interval)
			{
				value = value/interval;
				count = 0;

				temp_disk_write.add(String.format("%.2f", value));
				
				value = 0;
			}

			size--;
		}

		writeTwoValuesToFile(Constants.GRAPH_DATA_PATH + jobID + Constants.DISK_RW +".tsv", "Reads", "Writes", temp_disk_read, temp_disk_write);

		temp.clear();
		value = 0;
		count = 0;
		size = time.size() - 1;

		//Disk_readtime
		while (size > 0) {
			value += Double.parseDouble(disk_readtime.get(size));
			count++;

			if (count == interval)
			{
				value = value/interval;
				count = 0;

				temp_disk_readtime.add(String.format("%.2f", value));
				
				value = 0;
			}

			size--;
		}

		temp.clear();
		value = 0;
		count = 0;
		size = time.size() - 1;

		//Disk_writetime
		while (size > 0) {
			value += Double.parseDouble(disk_writetime.get(size));
			count++;

			if (count == interval)
			{
				value = value/interval;
				count = 0;

				temp_disk_writetime.add(String.format("%.2f", value));
				
				value = 0;
			}

			size--;
		}

		temp.clear();
		value = 0;
		count = 0;
		size = time.size() - 1;

		//Disk_iotime
		while (size > 0) {
			value += Double.parseDouble(disk_iotime.get(size));
			count++;

			if (count == interval)
			{
				value = value/interval;
				count = 0;

				temp_disk_iotime.add(String.format("%.2f", value));
				
				value = 0;
			}

			size--;
		}

		writeThreeValuesToFile(Constants.GRAPH_DATA_PATH + jobID + Constants.DISK_TIME +".tsv", "Read time", "Write time", "IO Time", temp_disk_readtime, temp_disk_writetime, temp_disk_iotime);

		temp.clear();
		value = 0;
		count = 0;
		size = time.size() - 1;

		//Memory
		while (size > 0) {
			value += Double.parseDouble(memory.get(size));
			count++;

			if (count == interval)
			{
				value = value/interval;
				count = 0;
				
				temp.add(String.format("%.2f", value));
				
				value = 0;
			}

			size--;
		}

		writeDBToFile(Constants.GRAPH_DATA_PATH + jobID + Constants.MEMORY +".tsv", temp);

		temp.clear();
		value = 0;
		count = 0;
		size = time.size() - 1;

		//Network_sent
		while (size > 0) {
			value += Double.parseDouble(network_sent.get(size));
			count++;

			if (count == interval)
			{
				value = value/interval;
				count = 0;

				temp_network_sent.add(String.format("%.2f", value));
				
				value = 0;
			}

			size--;
		}

		temp.clear();
		value = 0;
		count = 0;
		size = time.size() - 1;

		//Network_received
		while (size > 0) {
			value += Double.parseDouble(network_received.get(size));
			count++;

			if (count == interval)
			{
				value = value/interval;
				count = 0;

				temp.add(String.format("%.2f", value));
				
				value = 0;
			}

			size--;
		}

		writeTwoValuesToFile(Constants.GRAPH_DATA_PATH + jobID + Constants.NETWORK +".tsv", "Sent", "Received", temp_network_sent, temp);
	}

	/**
	 * writes a metric to File
	 * @param filename
	 */
	private void writeDBToFile(String filename, ArrayList<String> temp) {
		int size = time.size() - 1;
		int interval = (int)Math.ceil(size/Constants.MAXIMUM_DATA_VIEW);

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

	private void writeTwoValuesToFile(String filename, String title1, String title2, ArrayList<String> temp, ArrayList<String> temp1) {
		int size = time.size() - 1;
		int interval = (int)Math.ceil(size/Constants.MAXIMUM_DATA_VIEW);

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

	private void writeThreeValuesToFile(String filename, String title1, String title2, String title3, ArrayList<String> temp, ArrayList<String> temp1, ArrayList<String> temp2) {
		int size = time.size() - 1;
		int interval = (int)Math.ceil(size/Constants.MAXIMUM_DATA_VIEW);

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
		log.error("Start time " + start);
		log.error("End time " + end);

		long difference = etime.getTime() - stime.getTime(); 

		log.error("Diff " + difference);
		log.error("Diff 1 " + difference/1000);

		//Compute time
		long runningInterval = difference/1000; 
		//(Long.parseLong(JobSession.endTime) - Long.parseLong(JobSession.startTime))/ 1000; 

		System.out.println("Time interval " + runningInterval);

		time.clear();
		System.out.println("Original Time size " + time.size());

		for (int i = 0; i < runningInterval; i++) {
			time.add("" + i);
		}

		System.out.println("Before Time size " + time.size());
		System.out.println("Before CPU size " + cpu.size());
		System.out.println("Before Disk_read size " + disk_read.size());
		System.out.println("Before Disk_write size " + disk_write.size());
		System.out.println("Before Disk_readtime size " + disk_readtime.size());
		System.out.println("Before Disk_writetime size " + disk_writetime.size());
		System.out.println("Before Disk_iotime size " + disk_iotime.size());
		System.out.println("Before Memory size " + memory.size());
		System.out.println("Before Network_sent size " + network_sent.size());
		System.out.println("Before Network_received size " + network_received.size());

		System.out.println("Gap " + (runningInterval - cpu.size()));
		
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

		System.out.println("After Time size " + time.size());
		System.out.println("After CPU size " + cpu.size());
		System.out.println("After Disk_read size " + disk_read.size());
		System.out.println("After Disk_write size " + disk_write.size());
		System.out.println("After Disk_readtime size " + disk_readtime.size());
		System.out.println("After Disk_writetime size " + disk_writetime.size());
		System.out.println("After Disk_iotime size " + disk_iotime.size());
		System.out.println("After Memory size " + memory.size());
		System.out.println("After Network_sent size " + network_sent.size());
		System.out.println("After Network_received size " + network_received.size());

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

				if (type.equalsIgnoreCase(Constants.CPU))
				{
					cpu = temp;
				}
				else if (type.equalsIgnoreCase(Constants.DISK))
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

	public int getExperimentCount()
	{
		int count = 0;

		try {
			Statement statement = connection.createStatement();
			ResultSet rs = statement
					.executeQuery("Select count(jobid) from JobConfig");
			while (rs.next()) {
				count = Integer.parseInt(rs.getString("count(jobid)"));
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
}
