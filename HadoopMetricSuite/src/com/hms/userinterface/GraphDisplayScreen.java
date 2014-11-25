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
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;

import com.hms.common.Constants;
import com.hms.common.OldJob;
import com.hms.database.DatabaseManager;

public class GraphDisplayScreen {

	Combo combo;
	Combo combo_1;
	Browser browser;

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
				Constants.MEMORY, Constants.NETWORK, Constants.HADOOP_CONFIG});
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
			File f = new File("./dat/Default.html");
			browser.setUrl(f.getCanonicalPath());
		}
		catch (Exception e)
		{
			log.error("Default HTML exception", e);
		}
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
		if (combo_1.getText().equalsIgnoreCase(Constants.HADOOP_CONFIG))
		{
			File xmlFile = new File("./" + Constants.GRAPH_DATA_PATH + combo.getText().split(" on ")[0] + Constants.HADOOP_CONF_FILE);

			try
			{
				if (xmlFile.exists())
				{
					browser.setUrl(xmlFile.getCanonicalPath());
				}
				else
				{
					File f = new File("./dat/NoXML.html");
					browser.setUrl(f.getCanonicalPath());
				}
			}
			catch (Exception e)
			{
				log.error("XML exception", e);
			}
			return;
		}

		File htmlFile = new File("./" + Constants.GRAPH_DATA_PATH + combo.getText().split(" on ")[0] + combo_1.getText() + ".html");

		try
		{
			if (htmlFile.exists())
			{
				browser.setUrl(htmlFile.getCanonicalPath());
			}
			else
			{
				DatabaseManager data = new DatabaseManager();
				data.getConnection();
				data.fetchData(combo.getText().split(" on ")[0]);
				data.closeConnection();

				String templatePath = "";
				String toReplaceTemplate = "";

				if (combo_1.getText().equalsIgnoreCase(Constants.CPU) || combo_1.getText().equalsIgnoreCase(Constants.MEMORY))
				{
					templatePath = "./dat/Template.html";
					toReplaceTemplate = "Template.tsv";
				}
				else
				{
					templatePath = "./dat/Template2.html";
					toReplaceTemplate = "Template2.tsv";
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
					PrintWriter out = new PrintWriter(Constants.GRAPH_DATA_PATH + combo.getText().split(" on ")[0] + combo_1.getText() + ".html");
					out.print(htmlContent.replace(toReplaceTemplate, combo.getText().split(" on ")[0] + combo_1.getText() + ".tsv"));
					out.close();
				} finally {
					br.close();
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
