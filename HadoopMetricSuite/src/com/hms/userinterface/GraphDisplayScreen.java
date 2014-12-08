package com.hms.userinterface;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.eclipse.swt.SWT;
import org.eclipse.swt.browser.Browser;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;

import com.hms.common.Constants;
import com.hms.common.JobSession;
import com.hms.common.OldJob;
import com.hms.database.DatabaseManager;

public class GraphDisplayScreen {

	Combo combo;
	Combo combo_1;
	Browser browser;
	Button btnNodeWise;
	Button btnClusterWise;
	Button btnAllRuns;

	boolean isFirstTimeLoad = true;

	static final Logger log = (Logger) LogManager.getLogger(GraphDisplayScreen.class.getName());

	public void showGraph(Composite parent, ArrayList<OldJob> jobList)
	{
		String[] temp = new String[jobList.size()];

		int index = 0;

		for (OldJob job : jobList) {
			temp[index++] = job.jobid + " on " + job.startTime;
		}

		combo = new Combo(parent, SWT.NONE);
		combo.setItems(temp);
		combo.select(temp.length-1);
		combo.addSelectionListener(new SelectionAdapter() {
			@Override
			public void widgetSelected(SelectionEvent arg0) {
				loadGraph();
			}
		});
		FormData fd_combo = new FormData();
		fd_combo.width = 200;
		fd_combo.top = new FormAttachment(5);
		fd_combo.left = new FormAttachment(30);
		combo.setLayoutData(fd_combo);

		combo_1 = new Combo(parent, SWT.NONE);
		combo_1.setItems(new String[] { Constants.CPU, Constants.DISK_RW, Constants.DISK_TIME, 
				Constants.MEMORY, Constants.NETWORK, Constants.HADOOP_CONFIG, Constants.APP_METRICS});
		combo_1.select(0);
		combo_1.addSelectionListener(new SelectionAdapter() {
			@Override
			public void widgetSelected(SelectionEvent arg0) {
				loadGraph();
			}
		});

		FormData fd_combo_1 = new FormData();
		fd_combo_1.width = 150;
		fd_combo_1.top = new FormAttachment(combo, 0, SWT.TOP);
		fd_combo_1.left = new FormAttachment(combo, 79);
		combo_1.setLayoutData(fd_combo_1);

		browser = new Browser(parent, SWT.NONE);
		FormData fd_browser = new FormData();
		fd_browser.bottom = new FormAttachment(95);
		fd_browser.right = new FormAttachment(95);
		fd_browser.top = new FormAttachment(15);
		fd_browser.left = new FormAttachment(5);
		browser.setLayoutData(fd_browser);
		browser.setJavascriptEnabled(true);

		try
		{
			browser.setUrl(JobSession.getPathForResource("Default.html"));
		}
		catch (Exception e)
		{
			log.error("Default HTML exception", e);
		}

		btnNodeWise = new Button(parent, SWT.RADIO);
		FormData fd_btnNodeWise = new FormData();
		fd_btnNodeWise.top = new FormAttachment(combo, 0, SWT.TOP);
		fd_btnNodeWise.left = new FormAttachment(combo_1, 65);
		btnNodeWise.setLayoutData(fd_btnNodeWise);
		btnNodeWise.setText("Node wise");

		btnClusterWise = new Button(parent, SWT.RADIO);
		btnClusterWise.setSelection(true);
		btnClusterWise.addSelectionListener(new SelectionAdapter() {
			@Override
			public void widgetSelected(SelectionEvent arg0) {
				loadGraph();
			}
		});
		FormData fd_btnClusterWise = new FormData();
		fd_btnClusterWise.top = new FormAttachment(combo, 0, SWT.TOP);
		fd_btnClusterWise.left = new FormAttachment(btnNodeWise, 39);
		btnClusterWise.setLayoutData(fd_btnClusterWise);
		btnClusterWise.setText("Cluster wise");
		
		btnAllRuns = new Button(parent, SWT.CHECK);
		btnAllRuns.addSelectionListener(new SelectionAdapter() {
			@Override
			public void widgetSelected(SelectionEvent arg0) {
				loadGraph();
			}
		});
		FormData fd_btnAllRuns = new FormData();
		fd_btnAllRuns.left = new FormAttachment(btnClusterWise, 39);
		fd_btnAllRuns.top = new FormAttachment(combo, 0, SWT.TOP);
		btnAllRuns.setLayoutData(fd_btnAllRuns);
		btnAllRuns.setText("All runs");
	}

	public void refreshItems(ArrayList<OldJob> jobList)
	{
		String[] temp = new String[jobList.size()];

		int index = 0;

		for (OldJob job : jobList) {
			temp[index++] = job.jobid + " on " + job.startTime;
		}

		combo.setItems(temp);
		combo.select(temp.length-1);

		if (isFirstTimeLoad)
		{
			loadGraph();
			isFirstTimeLoad = false;
		}
	}

	private void loadGraph()
	{
		int runs = 1;
		boolean wantAllRun = false;
		
		try
		{
			DatabaseManager data = new DatabaseManager();
			data.getConnection();
			runs = data.getRunNumberForJob(combo.getText().split(" on ")[0]);
			data.closeConnection();
		}
		catch (Exception e)
		{
			log.error("Database run number fetch exception", e);
		}
		
		if (runs > 1)
		{
			btnAllRuns.setVisible(true);
		}
		else
		{
			btnAllRuns.setVisible(false);
		}
		
		if (combo_1.getText().equalsIgnoreCase(Constants.APP_METRICS))
		{
			File appMetricsFile = new File(JobSession.getGraphPath() + combo.getText().split(" on ")[0] + Constants.APP_LOG_NAME);

			try
			{
				if (appMetricsFile.exists())
				{
					browser.setUrl(appMetricsFile.getCanonicalPath());
				}
				else
				{
					File f = new File(JobSession.getPathForResource("NoAppMetrics.html"));
					browser.setUrl(f.getCanonicalPath());
				}
			}
			catch (Exception e)
			{
				log.error("XML exception", e);
			}
			return;
		}
		else if (combo_1.getText().equalsIgnoreCase(Constants.HADOOP_CONFIG))
		{
			File xmlFile = new File(JobSession.getGraphPath() + combo.getText().split(" on ")[0] + Constants.HADOOP_CONF_FILE);

			try
			{
				if (xmlFile.exists())
				{
					browser.setUrl(xmlFile.getCanonicalPath());
				}
				else
				{
					File f = new File(JobSession.getPathForResource("NoXML.html"));
					browser.setUrl(f.getCanonicalPath());
				}
			}
			catch (Exception e)
			{
				log.error("XML exception", e);
			}
			
			return;
		}
		
		String whichFileType = "";
		String fileName = combo.getText().split(" on ")[0];
		
		//Multiple runs of same experiment
		if (btnAllRuns.getVisible() && btnAllRuns.getSelection())
		{
			whichFileType = Constants.CLUSTER;
			fileName = combo.getText().split(" on ")[0].split(Constants.DELIMITER)[0];
			
			btnClusterWise.setVisible(false);
			btnNodeWise.setVisible(false);
		}
		else
		{
			btnClusterWise.setVisible(true);
			btnNodeWise.setVisible(true);
			
			if (btnClusterWise.getSelection())
			{
				whichFileType = Constants.CLUSTER;
			}
			else
			{
				whichFileType = Constants.NODE;
			}
		}
		
		wantAllRun = btnAllRuns.getSelection();
		
		log.info("Here Expected file name is " + JobSession.getGraphPath() + whichFileType + fileName + combo_1.getText() + ".html");

		File htmlFile = new File(JobSession.getGraphPath() + whichFileType + fileName + combo_1.getText() + ".html");

		try
		{
			if (htmlFile.exists())
			{
				log.info("HTML File exists");
				browser.setUrl(htmlFile.getCanonicalPath());
			}
			else
			{
				DatabaseManager data = new DatabaseManager();
				data.getConnection();
				data.fetchData(combo.getText().split(" on ")[0], whichFileType, wantAllRun, runs);
				data.closeConnection();

				String templatePath = "";
				String toReplaceTemplate = "";

				if (whichFileType == Constants.NODE)
				{
					templatePath = JobSession.getPathForResource("Template2.html");
					toReplaceTemplate = "Template2.tsv";
				}
				else
				{
					if (runs > 1 && btnAllRuns.getSelection())
					{
						templatePath = JobSession.getPathForResource("Template2.html");
						toReplaceTemplate = "Template2.tsv";
					}
					else if (combo_1.getText().equalsIgnoreCase(Constants.CPU) || combo_1.getText().equalsIgnoreCase(Constants.MEMORY))
					{
						templatePath = JobSession.getPathForResource("Template.html");
						toReplaceTemplate = "Template.tsv";
					}
					else
					{
						templatePath = JobSession.getPathForResource("Template2.html");
						toReplaceTemplate = "Template2.tsv";
					}
				}

				BufferedReader br = new BufferedReader(new FileReader(templatePath));
				try {
					StringBuilder sb = new StringBuilder();
					String line = br.readLine();

					while (line != null) {
						sb.append(line);
						sb.append(System.lineSeparator());
						line = br.readLine();
					}

					String htmlContent = sb.toString();
					htmlContent = htmlContent.replace(toReplaceTemplate, whichFileType + fileName + combo_1.getText() + ".tsv");

					if (combo_1.getText().equalsIgnoreCase(Constants.DISK_RW))
					{
						htmlContent = htmlContent.replace("Utilisation", "No.of reads/writes");
					}
					else if (combo_1.getText().equalsIgnoreCase(Constants.DISK_TIME))
					{
						htmlContent = htmlContent.replace("Utilisation", "Time spent in ms");
					}
					else if (combo_1.getText().equalsIgnoreCase(Constants.NETWORK))
					{
						htmlContent = htmlContent.replace("Utilisation", "No.of packets");
					}

					PrintWriter out = new PrintWriter(JobSession.getGraphPath() + whichFileType + fileName + combo_1.getText() + ".html");
					out.print(htmlContent);
					out.close();
				} finally {
					br.close();
					log.info("HTML Created and going to load now");
					browser.setUrl(htmlFile.getCanonicalPath());
				}
			}
		}
		catch (Exception e)
		{
			log.error("HTML generation exception", e);
		}
	}
}
