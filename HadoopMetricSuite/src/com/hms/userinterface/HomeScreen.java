package com.hms.userinterface;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.graphics.Font;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import com.hms.common.Constants;
import com.hms.common.JobSession;
import com.hms.common.UserLog;
import com.hms.connection.ClusterMaster;

public class HomeScreen {

	protected Shell shell;
	private Text textIPAddr;
	private Text textUsername;
	private Text textPassword;
	private Button btnOk;
	private Button btnReset;
	private boolean shallProceed = false;
	private String username;
	private String password;
	private String ipAddress;
	
	static final Logger log = (Logger) LogManager.getLogger(HomeScreen.class.getName());
	
	public static void main(String[] args) {
		HomeScreen home = new HomeScreen();
		home.displayHome();
	}

	public void displayHome()
	{
		JobSession.findOutOS();
		
		//Clean up old files
		JobSession.startUp();
		
		Display display = Display.getDefault();

		shell = new Shell();
		shell.setMinimumSize(1200, 700);
		shell.setText(Constants.APPLICATION_TITLE);
		shell.setLayout(new FormLayout());

		Label lblIPAddress = new Label(shell, SWT.NONE);
		lblIPAddress.setFont(new Font(Display.getCurrent(), Constants.fontData));
		FormData fd_lblIPAddress = new FormData();
		fd_lblIPAddress.top = new FormAttachment(34);
		fd_lblIPAddress.left = new FormAttachment(40);
		lblIPAddress.setLayoutData(fd_lblIPAddress);
		lblIPAddress.setText("IP Address");

		textIPAddr = new Text(shell, SWT.BORDER);
		textIPAddr.setFont(new Font(Display.getCurrent(), Constants.fontData));
		FormData fd_textIPAddr = new FormData();
		fd_textIPAddr.width = 150;
		fd_textIPAddr.height = 20;
		fd_textIPAddr.top = new FormAttachment(lblIPAddress, 0, SWT.CENTER);
		fd_textIPAddr.left = new FormAttachment(lblIPAddress, 20);
		textIPAddr.setLayoutData(fd_textIPAddr);

		Label lblUsername = new Label(shell, SWT.NONE);
		lblUsername.setFont(new Font(Display.getCurrent(), Constants.fontData));
		FormData fd_lblUsername = new FormData();
		fd_lblUsername.top = new FormAttachment(lblIPAddress, 30);
		fd_lblUsername.left = new FormAttachment(40);
		lblUsername.setLayoutData(fd_lblUsername);
		lblUsername.setText("Username");

		textUsername = new Text(shell, SWT.BORDER);
		textUsername.setFont(new Font(Display.getCurrent(), Constants.fontData));
		FormData fd_textUsername = new FormData();
		fd_textUsername.width = 150;
		fd_textUsername.height = 20;
		fd_textUsername.left = new FormAttachment(lblIPAddress, 20);
		fd_textUsername.top = new FormAttachment(lblUsername, 0, SWT.CENTER);
		textUsername.setLayoutData(fd_textUsername);

		Label lblPassword = new Label(shell, SWT.NONE);
		lblPassword.setFont(new Font(Display.getCurrent(), Constants.fontData));
		FormData fd_lblPassword = new FormData();
		fd_lblPassword.top = new FormAttachment(lblUsername, 30);
		fd_lblPassword.left = new FormAttachment(40);
		lblPassword.setLayoutData(fd_lblPassword);
		lblPassword.setText("Password");

		textPassword = new Text(shell, SWT.BORDER | SWT.PASSWORD);
		textPassword.setFont(new Font(Display.getCurrent(), Constants.fontData));
		FormData fd_textPassword = new FormData();
		fd_textPassword.width = 150;
		fd_textPassword.height = 20;
		fd_textPassword.top = new FormAttachment(lblPassword, 0, SWT.CENTER);
		fd_textPassword.left = new FormAttachment(lblIPAddress, 20);
		textPassword.setLayoutData(fd_textPassword);

		btnOk = new Button(shell, SWT.NONE);
		btnOk.setFont(new Font(Display.getCurrent(), Constants.fontData));
		FormData fd_btnOk = new FormData();
		fd_btnOk.width = 75;
		fd_btnOk.height = 35;
		fd_btnOk.top = new FormAttachment(lblPassword, 30);
		fd_btnOk.left = new FormAttachment(40);
		btnOk.setLayoutData(fd_btnOk);
		btnOk.addSelectionListener(new SelectionAdapter() {
			@Override
			public void widgetSelected(SelectionEvent arg0) {

				setWidgetEnabled(false);
				
				ipAddress = textIPAddr.getText();
				username = textUsername.getText();
				password = textPassword.getText();

				Thread checkDetailsThread = new Thread(new Runnable() {
					public void run()
					{
						checkDetails();
						updateUI();
					}});  
				checkDetailsThread.start();
			}
		});
		btnOk.setText("Ok");

		btnReset = new Button(shell, SWT.NONE);
		btnReset.setFont(new Font(Display.getCurrent(), Constants.fontData));
		FormData fd_btnReset = new FormData();
		fd_btnReset.right = new FormAttachment(textPassword, 0, SWT.RIGHT);
		fd_btnReset.height = 35;
		fd_btnReset.width = 75;
		fd_btnReset.top = new FormAttachment(btnOk, 0, SWT.TOP);
		btnReset.setLayoutData(fd_btnReset);
		btnReset.addSelectionListener(new SelectionAdapter() {
			@Override
			public void widgetSelected(SelectionEvent arg0) {

				textIPAddr.setText("");
				textUsername.setText("");
				textPassword.setText("");
			}
		});
		btnReset.setText("Reset");

		shell.pack();
		shell.open();
		
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch())
				display.sleep();
		}
		
		display.dispose();
	}

	private void checkDetails()
	{
		ClusterMaster master = new ClusterMaster();

		if (master.connectToMaster(ipAddress, username, password))
		{
			master.disconnectMaster();

			JobSession.masterIP = ipAddress;
			JobSession.username = username;
			JobSession.password = password;

			shallProceed = true;
		}
	}
	
	private void chooseOutputPath()
	{
		DirectoryDialog dialog = new DirectoryDialog(shell, SWT.APPLICATION_MODAL);
		dialog.setMessage("Please choose a directory for the application to store metrics");
		dialog.setText(Constants.APPLICATION_TITLE);
		
		String str = dialog.open();

		if (str == null)
		{
		    File dir = new File(System.getProperty("user.home") + "/Desktop/" + "HMS");
		    dir.mkdir();
		    
		    JobSession.hmsPath = System.getProperty("user.home") + "/Desktop/HMS/";
			
			MessageBox messageBox = new MessageBox(shell, SWT.ICON_INFORMATION | SWT.IGNORE);
			messageBox.setMessage("A directory called HMS has been created in the dekstop to store the metrics");
			messageBox.open();
		}
		else
		{
			JobSession.hmsPath = str;
	    	JobSession.hmsPath += "/";
		}
		
	    log.info("HMS Path " + JobSession.hmsPath);
	    
	    try
	    {
	    	JobSession.exportResourcesFromJAR();
	    }
	    catch (Exception e)
	    {
	    	log.error("Excepiton in exporting sqlite outside jar", e);
	    }
	    
	    File dir = new File(JobSession.hmsPath + "graph");
	    dir.mkdir();
	}

	private void updateUI()
	{
		Display.getDefault().syncExec(new Runnable(){

			public void run(){

				if (shallProceed)
				{
					chooseOutputPath();
					
					shell.close();
					
					UserLog.getUserLog();

					TabbedScreen tab = new TabbedScreen();
					tab.displayTabbedScreen();
				}
				else
				{
					MessageBox messageBox = new MessageBox(shell, SWT.ICON_WARNING | SWT.IGNORE);
					messageBox.setText("Warning");
					messageBox.setMessage(UserLog.getUserLog());
					messageBox.open();
				}
			}
		});
	}

	private void setWidgetEnabled(boolean enabled)
	{
		btnOk.setEnabled(enabled);
		btnReset.setEnabled(enabled);
	}
}

