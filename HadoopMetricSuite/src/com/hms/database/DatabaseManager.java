package com.hms.database;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;

import com.hms.common.Constants;
import com.hms.common.JobSession;

/**
 * @author pratyushatiruveedhula
 * 
 */
public class DatabaseManager {
	Connection connection = null;
	private static ArrayList<String> time = new ArrayList<>();
	private static ArrayList<String> cpu = new ArrayList<>();
	private static ArrayList<String> disk = new ArrayList<>();
	private static ArrayList<String> memory = new ArrayList<>();
	private static ArrayList<String> network = new ArrayList<>();

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

		time.clear();
		cpu.clear();
		disk.clear();
		memory.clear();
		network.clear();

		try {
			Statement statement = connection.createStatement();
			ResultSet rs = statement
					.executeQuery("SELECT timestamp, avg(CPU), avg(Disk), avg(Memory), avg(Network) FROM PlatformMetrics where jobid='" + jobID +"' group by timestamp");
			while (rs.next()) {
				time.add( rs.getString("timestamp"));
				cpu.add( rs.getString("avg(CPU)"));
				disk.add( rs.getString("avg(Disk)"));
				memory.add( rs.getString("avg(Memory)"));
				network.add( rs.getString("avg(Network)"));
			}
		} catch (SQLException e) {
			// connection close failed.
			log.error("Database fetch exception", e);
			throw e;
		}
		
		writeDBToFile("dat/" + jobID + Constants.CPU +".tsv", cpu);
		writeDBToFile("dat/" + jobID + Constants.DISK +".tsv", disk);
		writeDBToFile("dat/" + jobID + Constants.MEMORY +".tsv", memory);
		writeDBToFile("dat/" + jobID + Constants.NETWORK +".tsv", network);
	}

	/**
	 * writes a metric to File
	 * @param filename
	 */
	private void writeDBToFile(String filename, ArrayList<String> temp) {
		try {
			FileWriter fileWriter = new FileWriter(filename);
			BufferedWriter writer = new BufferedWriter(fileWriter);
			writer.write("letter\tfrequency\n");
			for (int i = 0; i < time.size(); i++) {
				writer.append(time.get(i) + "\t" + temp.get(i) + "\n");
			}
			writer.close();
		} catch (IOException e) {
			log.error("Database file writing exception", e);
		}
	}

	/**
	 * @throws SQLException
	 */
	public void insertIntoJobConfig() throws SQLException {
		try {
			String values = "'" + JobSession.jobID + "','" + JobSession.nodes + "','" + JobSession.datasize +"','" + JobSession.startTime +"','" + JobSession.endTime +"'";
			Statement stmt = connection.createStatement();
			stmt.executeUpdate("INSERT INTO JobConfig (JobId, Nodes, DataSize, StartTime, EndTime) VALUES(" + values +")");
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
		
		//List will be added with time from all logs and hence the trimming
		time.subList(cpu.size(), cpu.size() * 4).clear();
		
		try {
			String query = null;
			Statement stmt= connection.createStatement();
			
			for (int i = 0; i < time.size(); i++) {
				query = "INSERT INTO PlatformMetrics VALUES ("
						+ time.get(i) + ","
						+ "'" + JobSession.jobID + "',"
						+ cpu.get(i) + ","
						+ disk.get(i) + ","
						+ memory.get(i) + ","
						+ network.get(i) + ","
						+ "'" + nodeID + "')";
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

					if (lineContent.length != 2)
					{
						line = bufferReadForFile.readLine();
						continue;
					}

					if (Long.parseLong(lineContent[0]) >= Long.parseLong(JobSession.startTime) && Long.parseLong(lineContent[0]) <= Long.parseLong(JobSession.endTime))
					{
						time.add(lineContent[0]);
						temp.add(lineContent[1]);
					}

					line = bufferReadForFile.readLine();
				}
				
				if (type.equalsIgnoreCase(Constants.CPU))
				{
					cpu = temp;
				}
				else if (type.equalsIgnoreCase(Constants.DISK))
				{
					disk = temp;
				}
				else if (type.equalsIgnoreCase(Constants.MEMORY))
				{
					memory = temp;
				}
				else if (type.equalsIgnoreCase(Constants.NETWORK))
				{
					network = temp;
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
	
	public ArrayList<String> getOldJobs()
	{
		ArrayList<String> temp = new ArrayList<>();
		
		try {
			Statement statement = connection.createStatement();
			ResultSet rs = statement
					.executeQuery("Select jobid, starttime from JobConfig");
			while (rs.next()) {
				temp.add(rs.getString("jobid") + " on " + rs.getString("starttime"));
			}
		} catch (SQLException e) {
			// connection close failed.
			log.error("Database fetch exception", e);
		}
		
		return temp;
	}
}
