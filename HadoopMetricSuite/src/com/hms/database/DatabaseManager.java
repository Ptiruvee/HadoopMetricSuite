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

/**
 * @author pratyushatiruveedhula
 * 
 */
public class DatabaseManager {
	Connection connection = null;
	private static String[] time = new String[200];
	private static String[] cpu = new String[200];

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
			System.err.println(e.getMessage());
			throw e;
		}
	}

	/**
	 * fetches data from the database
	 * @throws SQLException
	 */
	public void fetchData() throws SQLException {
		try {
			Statement statement = connection.createStatement();
			ResultSet rs = statement
					.executeQuery("SELECT timestamp, CPU FROM PlatformMetrics");
			int index = 0;
			while (rs.next()) {
				System.out.println("Read " + rs.getString(1));
				System.out.println("Read " + rs.getString(2));
				System.out.println("Read " + rs.getString("timestamp"));
				System.out.println("Read " + rs.getString("CPU"));
				time[index] = rs.getString("timestamp");
				cpu[index] = rs.getString("CPU");
				index++;
				if (index == 200) {
					break;
				}
			}
		} catch (SQLException e) {
			// connection close failed.
			System.err.println(e);
			throw e;
		}

		writeCPUToFile("dat/data2.tsv");
	}

	/**
	 * writes a metric to File
	 * @param filename
	 */
	public static void writeCPUToFile(String filename) {
		try {
			FileWriter fileWriter = new FileWriter(filename);
			BufferedWriter writer = new BufferedWriter(fileWriter);
			writer.write("letter\tfrequency\n");
			for (int i = 0; i < time.length; i++) {
				writer.append(time[i] + "\t" + cpu[i] + "\n");
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * @throws SQLException
	 */
	public void insertIntoJobConfig() throws SQLException {
		try {
			Statement stmt = connection.createStatement();
			stmt.executeUpdate("INSERT INTO JobConfig (JobId, Nodes, DataSize, StartTime, EndTime) VALUES('1','2','8','1','1')");
			connection.commit();
		} catch (SQLException e) {
			// connection close failed.
			System.err.println(e);
			throw e;
		}
	}

	/**
	 * @throws IOException
	 */
	public void insertIntoPlatformMetrics() throws IOException {
		try {
			BufferedReader bufferReadForFile = new BufferedReader(
					new FileReader("dat/Log.txt"));
			try {
				String line = bufferReadForFile.readLine();
				int jobID = 1;
				int time = 1;
				String query = null;
				Statement stmt= connection.createStatement();
				while (line != null) {
					if (line.length() == 0) {
						line = bufferReadForFile.readLine();
						continue;
					}
					query = "INSERT INTO PlatformMetrics VALUES ("
							+ Integer.toString(time++) + ","
							+ Integer.toString(jobID) + ","
							+ Float.parseFloat(line.replace("%", ""))
							+ ", 0, 0, 0, 0" + ")";
					stmt.addBatch(query);
					line = bufferReadForFile.readLine();
				}
				stmt.executeBatch();
			} catch (IOException fileException) {
				System.out
						.println("\n There is a problem with the input file or path, please look the following trace to rectify it");
				fileException.printStackTrace();
			} finally {
				bufferReadForFile.close();
			}
			connection.commit();

		} catch (SQLException e) {
			// connection close failed.
			System.err.println(e);
		}
	}

}
