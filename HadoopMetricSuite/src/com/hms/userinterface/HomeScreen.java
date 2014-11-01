package com.hms.userinterface;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.BusyIndicator;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

import com.hms.common.UserLog;
import com.hms.connection.ClusterMaster;

public class HomeScreen {

	protected Shell shell;
	private Text textIPAddr;
	private Text textUsername;
	private Text textPassword;
	
	public void displayHome()
	{
		Display display = Display.getDefault();
		
		shell = new Shell();
		shell.setMinimumSize(500, 500);
		shell.setText("SWT Application");
		shell.setLayout(new FormLayout());
		
		Label lblIPAddress = new Label(shell, SWT.NONE);
		FormData fd_lblIPAddress = new FormData();
		fd_lblIPAddress.top = new FormAttachment(34);
		fd_lblIPAddress.left = new FormAttachment(25);
		lblIPAddress.setLayoutData(fd_lblIPAddress);
		lblIPAddress.setText("IP Address");
		
		textIPAddr = new Text(shell, SWT.BORDER);
		FormData fd_textIPAddr = new FormData();
		fd_textIPAddr.width = 150;
		fd_textIPAddr.height = 20;
		fd_textIPAddr.top = new FormAttachment(lblIPAddress, 0, SWT.CENTER);
		fd_textIPAddr.left = new FormAttachment(lblIPAddress, 20);
		textIPAddr.setLayoutData(fd_textIPAddr);
		
		Label lblUsername = new Label(shell, SWT.NONE);
		FormData fd_lblUsername = new FormData();
		fd_lblUsername.top = new FormAttachment(lblIPAddress, 30);
		fd_lblUsername.left = new FormAttachment(25);
		lblUsername.setLayoutData(fd_lblUsername);
		lblUsername.setText("Username");
		
		textUsername = new Text(shell, SWT.BORDER);
		FormData fd_textUsername = new FormData();
		fd_textUsername.width = 150;
		fd_textUsername.height = 20;
		fd_textUsername.left = new FormAttachment(lblIPAddress, 20);
		fd_textUsername.top = new FormAttachment(lblUsername, 0, SWT.CENTER);
		textUsername.setLayoutData(fd_textUsername);
		
		Label lblPassword = new Label(shell, SWT.NONE);
		FormData fd_lblPassword = new FormData();
		fd_lblPassword.top = new FormAttachment(lblUsername, 30);
		fd_lblPassword.left = new FormAttachment(25);
		lblPassword.setLayoutData(fd_lblPassword);
		lblPassword.setText("Password");
		
		textPassword = new Text(shell, SWT.BORDER | SWT.PASSWORD);
		FormData fd_textPassword = new FormData();
		fd_textPassword.width = 150;
		fd_textPassword.height = 20;
		fd_textPassword.top = new FormAttachment(lblPassword, 0, SWT.CENTER);
		fd_textPassword.left = new FormAttachment(lblIPAddress, 20);
		textPassword.setLayoutData(fd_textPassword);
		
		Button btnOk = new Button(shell, SWT.NONE);
		FormData fd_btnOk = new FormData();
		fd_btnOk.width = 75;
		fd_btnOk.height = 20;
		fd_btnOk.top = new FormAttachment(lblPassword, 30);
		fd_btnOk.left = new FormAttachment(25);
		btnOk.setLayoutData(fd_btnOk);
		btnOk.addSelectionListener(new SelectionAdapter() {
			@Override
			public void widgetSelected(SelectionEvent arg0) {
				
				BusyIndicator.showWhile(Display.getDefault(), new Runnable(){

				    public void run(){
				    	
				    	ClusterMaster master = new ClusterMaster();
				    	
				    	if (master.connectToMaster(textIPAddr.getText(), textUsername.getText(), textPassword.getText()))
				    	{
				    		master.disconnectMaster();
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
		});
		btnOk.setText("Ok");
		
		Button btnReset = new Button(shell, SWT.NONE);
		FormData fd_btnReset = new FormData();
		fd_btnReset.right = new FormAttachment(textPassword, 0, SWT.RIGHT);
		fd_btnReset.height = 20;
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
}

