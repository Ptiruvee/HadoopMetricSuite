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
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

import com.hms.common.Constants;
import com.hms.database.DatabaseManager;

public class GraphDisplayScreen {

	protected Shell shlHadoopMetricsSuite;
	Combo combo;
	Combo combo_1;
	Browser browser;
	
	static final Logger log = (Logger) LogManager.getLogger(GraphDisplayScreen.class.getName());

	public void showGraph(ArrayList<String> jobList)
	{
		String[] temp = new String[jobList.size()];

		for (int i = 0; i < jobList.size(); i++) {
			temp[i] = jobList.get(i);
		}
		Display display = Display.getDefault();

		shlHadoopMetricsSuite = new Shell();
		shlHadoopMetricsSuite.setMinimumSize(new Point(1100, 600));
		shlHadoopMetricsSuite.setSize(450, 300);
		shlHadoopMetricsSuite.setText(Constants.APPLICATION_TITLE);
		shlHadoopMetricsSuite.setLayout(new FormLayout());

		combo = new Combo(shlHadoopMetricsSuite, SWT.NONE);
		combo.setItems(temp);
		combo.addSelectionListener(new SelectionAdapter() {
			@Override
			public void widgetSelected(SelectionEvent arg0) {
			}
		});
		FormData fd_combo = new FormData();
		fd_combo.width = 200;
		fd_combo.top = new FormAttachment(5);
		fd_combo.left = new FormAttachment(30);
		combo.setLayoutData(fd_combo);

		combo_1 = new Combo(shlHadoopMetricsSuite, SWT.NONE);
		combo_1.setItems(new String[] { Constants.CPU, Constants.DISK,
				Constants.MEMORY, Constants.NETWORK });
		combo_1.addSelectionListener(new SelectionAdapter() {
			@Override
			public void widgetSelected(SelectionEvent arg0) {
				try
				{
					DatabaseManager data = new DatabaseManager();
					data.getConnection();
					data.fetchData(combo.getText().split(" on ")[0]);
					data.closeConnection();
					
					BufferedReader br = new BufferedReader(new FileReader("./dat/Template.html"));
					try {
						StringBuilder sb = new StringBuilder();
						String line = br.readLine();

						while (line != null) {
							sb.append(line);
							sb.append(System.lineSeparator());
							line = br.readLine();
						}
						String htmlContent = sb.toString();
						PrintWriter out = new PrintWriter("dat/" + combo.getText().split(" on ")[0] + combo_1.getText() + ".html");
						out.print(htmlContent.replace("Template.tsv", combo.getText().split(" on ")[0] + combo_1.getText() + ".tsv"));
						out.close();
					} finally {
						br.close();
						File f = new File("./dat/" + combo.getText().split(" on ")[0] + combo_1.getText() + ".html");
						browser.setUrl(f.getCanonicalPath());
					}
				}
				catch (Exception e)
				{
					log.error("HTML generation exception", e);
				}
			}
		});
		FormData fd_combo_1 = new FormData();
		fd_combo_1.width = 150;
		fd_combo_1.top = new FormAttachment(combo, 0, SWT.TOP);
		fd_combo_1.left = new FormAttachment(combo, 79);
		combo_1.setLayoutData(fd_combo_1);

		browser = new Browser(shlHadoopMetricsSuite, SWT.NONE);
		FormData fd_browser = new FormData();
		fd_browser.bottom = new FormAttachment(95);
		fd_browser.right = new FormAttachment(90);
		fd_browser.top = new FormAttachment(15);
		fd_browser.left = new FormAttachment(10);
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

		shlHadoopMetricsSuite.open();
		shlHadoopMetricsSuite.layout();
		while (!shlHadoopMetricsSuite.isDisposed()) {
			if (!display.readAndDispatch()) {
				display.sleep();
			}
		}
		display.dispose();
	}
}
