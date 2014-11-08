package com.hms.userinterface;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import com.hms.common.Constants;
import com.hms.common.JobSession;
import com.hms.common.UserLog;
import com.hms.connection.ClusterMaster;

public class ConfigurationScreen {

	protected Shell shlHadoopMetricsSuite;
	private Text text;
	private Text text_1;
	private Text text_2;
	private Text text_3;
	private Combo combo;
	private Combo combo_1;
	private Button btnStart;
	private Button btnShowGraph;
	private Button btnReset;
	private boolean shouldUpdateLog;

	static final Logger log = (Logger) LogManager
			.getLogger(ConfigurationScreen.class.getName());

	public void displayConfigScreen(String experimentName) {
		Display display = Display.getDefault();

		shlHadoopMetricsSuite = new Shell();
		shlHadoopMetricsSuite.setMinimumSize(new Point(1100, 600));
		shlHadoopMetricsSuite.setSize(450, 300);
		shlHadoopMetricsSuite.setText("Hadoop Metrics Suite");
		shlHadoopMetricsSuite.setLayout(new FormLayout());

		text = new Text(shlHadoopMetricsSuite, SWT.BORDER);
		text.setEditable(false);
		FormData fd_text = new FormData();
		fd_text.height = 400;
		fd_text.bottom = new FormAttachment(0, 449);
		fd_text.right = new FormAttachment(95);
		fd_text.top = new FormAttachment(15);
		fd_text.left = new FormAttachment(0, 494);
		text.setLayoutData(fd_text);

		Composite composite = new Composite(shlHadoopMetricsSuite, SWT.NONE);
		composite.setToolTipText("Seconds");
		FormData fd_composite = new FormData();
		fd_composite.height = 400;
		fd_composite.right = new FormAttachment(0, 432);
		fd_composite.top = new FormAttachment(10);
		fd_composite.left = new FormAttachment(5);
		composite.setLayoutData(fd_composite);
		composite.setLayout(new FormLayout());

		Label lblNewLabel = new Label(composite, SWT.NONE);
		FormData fd_lblNewLabel = new FormData();
		fd_lblNewLabel.top = new FormAttachment(0, 26);
		fd_lblNewLabel.left = new FormAttachment(0, 10);
		lblNewLabel.setLayoutData(fd_lblNewLabel);
		lblNewLabel.setText(experimentName);

		Label lblNewLabel_1 = new Label(composite, SWT.NONE);
		FormData fd_lblNewLabel_1 = new FormData();
		fd_lblNewLabel_1.top = new FormAttachment(lblNewLabel, 50);
		fd_lblNewLabel_1.left = new FormAttachment(0, 10);
		lblNewLabel_1.setLayoutData(fd_lblNewLabel_1);
		lblNewLabel_1.setText("Select the application type");

		Label lblEnterTheVolume = new Label(composite, SWT.NONE);
		FormData fd_lblEnterTheVolume = new FormData();
		fd_lblEnterTheVolume.top = new FormAttachment(lblNewLabel_1, 40);
		fd_lblEnterTheVolume.left = new FormAttachment(lblNewLabel, 0, SWT.LEFT);
		lblEnterTheVolume.setLayoutData(fd_lblEnterTheVolume);
		lblEnterTheVolume
				.setText("Enter the volume of input data\n*Maximum data 8 GB");

		Label lblNewLabel_2 = new Label(composite, SWT.NONE);
		FormData fd_lblNewLabel_2 = new FormData();
		fd_lblNewLabel_2.top = new FormAttachment(lblEnterTheVolume, 41);
		fd_lblNewLabel_2.left = new FormAttachment(lblNewLabel, 0, SWT.LEFT);
		lblNewLabel_2.setLayoutData(fd_lblNewLabel_2);
		lblNewLabel_2.setText("Frequency of retrieval");

		Label lblNewLabel_3 = new Label(composite, SWT.NONE);
		FormData fd_lblNewLabel_3 = new FormData();
		fd_lblNewLabel_3.top = new FormAttachment(lblNewLabel_2, 39);
		fd_lblNewLabel_3.left = new FormAttachment(lblNewLabel, 0, SWT.LEFT);
		lblNewLabel_3.setLayoutData(fd_lblNewLabel_3);
		lblNewLabel_3.setText("Select number of runs");

		Label lblNewLabel_4 = new Label(composite, SWT.NONE);
		FormData fd_lblNewLabel_4 = new FormData();
		fd_lblNewLabel_4.top = new FormAttachment(lblNewLabel_3, 39);
		fd_lblNewLabel_4.left = new FormAttachment(lblNewLabel, 0, SWT.LEFT);
		lblNewLabel_4.setLayoutData(fd_lblNewLabel_4);
		lblNewLabel_4.setText("Select a metric to display");

		combo = new Combo(composite, SWT.NONE);
		combo.setItems(new String[] { Constants.WORD_COUNT, Constants.SORT,
				Constants.GREP, Constants.DEDUP });
		combo.select(0);
		FormData fd_combo = new FormData();
		fd_combo.width = 150;
		fd_combo.bottom = new FormAttachment(lblNewLabel_1, 0, SWT.BOTTOM);
		fd_combo.left = new FormAttachment(lblNewLabel_1, 61);
		combo.setLayoutData(fd_combo);

		text_1 = new Text(composite, SWT.BORDER);
		text_1.setText("1");
		text_1.setToolTipText("GB");
		FormData fd_text_1 = new FormData();
		fd_text_1.width = 140;
		fd_text_1.bottom = new FormAttachment(lblEnterTheVolume, 0, SWT.BOTTOM);
		fd_text_1.left = new FormAttachment(combo, 0, SWT.LEFT);
		text_1.setLayoutData(fd_text_1);

		text_2 = new Text(composite, SWT.BORDER);
		text_2.setText("1");
		FormData fd_text_2 = new FormData();
		fd_text_2.width = 140;
		fd_text_2.bottom = new FormAttachment(lblNewLabel_2, 0, SWT.BOTTOM);
		fd_text_2.left = new FormAttachment(combo, 0, SWT.LEFT);
		text_2.setLayoutData(fd_text_2);

		text_3 = new Text(composite, SWT.BORDER);
		text_3.setText("1");
		FormData fd_text_3 = new FormData();
		fd_text_3.width = 140;
		fd_text_3.bottom = new FormAttachment(lblNewLabel_3, 0, SWT.BOTTOM);
		fd_text_3.left = new FormAttachment(combo, 0, SWT.LEFT);
		text_3.setLayoutData(fd_text_3);

		combo_1 = new Combo(composite, SWT.NONE);
		combo_1.setItems(new String[] { Constants.CPU, Constants.CPU_PROCESS,
				Constants.DISK, Constants.MEMORY, Constants.NETWORK });
		combo_1.select(0);
		FormData fd_combo_1 = new FormData();
		fd_combo_1.width = 150;
		fd_combo_1.bottom = new FormAttachment(lblNewLabel_4, 0, SWT.BOTTOM);
		fd_combo_1.left = new FormAttachment(combo, 0, SWT.LEFT);
		combo_1.setLayoutData(fd_combo_1);

		Label label = new Label(shlHadoopMetricsSuite, SWT.SEPARATOR
				| SWT.VERTICAL);
		FormData fd_label = new FormData();
		fd_label.height = 400;
		fd_label.left = new FormAttachment(composite, 35);
		fd_label.top = new FormAttachment(composite, 0, SWT.TOP);
		label.setLayoutData(fd_label);

		btnStart = new Button(shlHadoopMetricsSuite, SWT.NONE);
		btnStart.addSelectionListener(new SelectionAdapter() {
			@Override
			public void widgetSelected(SelectionEvent arg0) {

				System.out.println("Application " + combo.getText());
				System.out.println("Data size " + text_1.getText());
				System.out.println("Frequency " + text_2.getText());
				System.out.println("Run " + text_3.getText());
				System.out.println("Graph " + combo_1.getText());

				setWidgetEnabled(false);

				Thread hadoopJobThread = new Thread(new Runnable() {
					public void run() {
						initiateHadoopJob();
					}
				});
				hadoopJobThread.start();
			}
		});
		FormData fd_btnStart = new FormData();
		fd_btnStart.top = new FormAttachment(composite, 44);
		fd_btnStart.left = new FormAttachment(0, 336);
		btnStart.setLayoutData(fd_btnStart);
		btnStart.setText("Start");

		btnShowGraph = new Button(shlHadoopMetricsSuite, SWT.NONE);
		btnShowGraph.addSelectionListener(new SelectionAdapter() {
			@Override
			public void widgetSelected(SelectionEvent arg0) {
			}
		});
		FormData fd_btnShowGraph = new FormData();
		fd_btnShowGraph.bottom = new FormAttachment(btnStart, 0, SWT.BOTTOM);
		fd_btnShowGraph.left = new FormAttachment(btnStart, 70);
		btnShowGraph.setLayoutData(fd_btnShowGraph);
		btnShowGraph.setText("Show graph");

		btnReset = new Button(shlHadoopMetricsSuite, SWT.NONE);
		btnReset.addSelectionListener(new SelectionAdapter() {
			@Override
			public void widgetSelected(SelectionEvent arg0) {
				text.setText("");
				text_1.setText("");
				text_2.setText("");
				text_3.setText("");
				combo.setText("");
				combo_1.setText("");
			}
		});
		FormData fd_btnReset = new FormData();
		fd_btnReset.top = new FormAttachment(btnStart, 0, SWT.TOP);
		fd_btnReset.left = new FormAttachment(btnShowGraph, 90);
		btnReset.setLayoutData(fd_btnReset);
		btnReset.setText("Reset");

		shlHadoopMetricsSuite.open();
		shlHadoopMetricsSuite.layout();
		while (!shlHadoopMetricsSuite.isDisposed()) {
			if (!display.readAndDispatch()) {
				display.sleep();
			}
		}
		display.dispose();
	}

	private void initiateHadoopJob() {

		Display.getDefault().asyncExec(new Runnable() {

			public void run() {
				if (!verifyInput()) {
					setWidgetEnabled(true);

					MessageBox messageBox = new MessageBox(
							shlHadoopMetricsSuite, SWT.ICON_WARNING
									| SWT.IGNORE);
					messageBox.setText("Warning");
					messageBox
							.setMessage("There is something wrong with the input, please verify!");
					messageBox.open();

					return;
				}
			}
		});

		shouldUpdateLog = true;

		Thread updateLogThread = new Thread(new Runnable() {
			public void run() {
				while (shouldUpdateLog) {
					Display.getDefault().asyncExec(new Runnable() {

						public void run() {
							text.append(UserLog.getUserLog());
						}
					});

					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						log.error("Log update thread sleep exception", e);
					}
				}
			}
		});
		updateLogThread.start();

		ClusterMaster master = new ClusterMaster();

		if (master.connectToMaster(JobSession.masterIP, JobSession.username,
				JobSession.password)) {
			// master.fetchSlaveList();
			// master.transferAndRunScriptFile();
			// master.runApplicationJob(Constants.WORD_COUNT);
			master.disconnectMaster();
		}

		shouldUpdateLog = false;

		Display.getDefault().asyncExec(new Runnable() {

			public void run() {
				// Just to make sure last of log is shown to the user
				text.append(UserLog.getUserLog());
				setWidgetEnabled(true);
			}
		});
	}

	private void setWidgetEnabled(boolean enabled) {
		btnStart.setEnabled(enabled);
		btnShowGraph.setEnabled(enabled);
		btnReset.setEnabled(enabled);
	}

	private boolean verifyInput() {
		boolean isInputCorrect = false;

		try {
			log.info(Integer.parseInt(text_1.getText()));
			log.info(Integer.parseInt(text_2.getText()));
			log.info(Integer.parseInt(text_3.getText()));
			
			JobSession.retrievalFrequency = Integer.parseInt(text_2.getText());

			isInputCorrect = true;
		} catch (Exception e) {
			log.error("Configuration input excpetion ", e);
		}

		return isInputCorrect;
	}
}