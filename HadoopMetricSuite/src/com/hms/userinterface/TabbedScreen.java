package com.hms.userinterface;

import java.util.ArrayList;

import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.TabFolder;
import org.eclipse.swt.widgets.TabItem;

import com.hms.common.Constants;
import com.hms.database.DatabaseManager;

public class TabbedScreen {
	
	TabFolder tabFolder;
	TabItem tbtmJobs;
	TabItem tbtmGraph;

	public void displayTabbedScreen() {
		Display display = Display.getDefault();
		
		Shell shell = new Shell();
		shell.setMinimumSize(new Point(1200, 700));
		shell.setSize(450, 300);
		shell.setText(Constants.APPLICATION_TITLE);
		shell.setLayout(new FillLayout(SWT.HORIZONTAL));
		
		tabFolder = new TabFolder(shell, SWT.NONE);
		
		tbtmJobs = new TabItem(tabFolder, SWT.NONE);
		tbtmJobs.setText("Jobs");
		
		tbtmGraph = new TabItem(tabFolder, SWT.NONE);
		tbtmGraph.setText("Graph");
		
		makeConfigureScreen();
		
		makeGraphScreen();

		shell.open();
		shell.layout();
		while (!shell.isDisposed()) {
			if (!display.readAndDispatch()) {
				display.sleep();
			}
		}
	}
	
	private void makeConfigureScreen()
	{
		Composite configurationComposite = new Composite(tabFolder, SWT.NONE);
		FormData fd_composite = new FormData();
		fd_composite.height = 400;
		fd_composite.top = new FormAttachment(10);
		fd_composite.left = new FormAttachment(5);
		configurationComposite.setLayoutData(fd_composite);
		configurationComposite.setLayout(new FormLayout());
		
		int expNo = 0;
		
		try
		{
			DatabaseManager dbManager = new DatabaseManager();
			dbManager.getConnection();
			expNo = dbManager.getExperimentCount() + 1;
			dbManager.closeConnection();
		}
		catch (Exception e)
		{
			System.out.println("Exception inside experiment count fetch");
			e.printStackTrace();
		}
		
		ConfigurationScreen config = new ConfigurationScreen();
		config.displayConfigScreen(configurationComposite, "Experiment " + expNo);
		
		tbtmJobs.setControl(configurationComposite);
	}
	
	private void makeGraphScreen()
	{
		Composite graphComposite = new Composite(tabFolder, SWT.NONE);
		FormData fd_composite = new FormData();
		fd_composite.height = 400;
		fd_composite.top = new FormAttachment(10);
		fd_composite.left = new FormAttachment(5);
		graphComposite.setLayoutData(fd_composite);
		graphComposite.setLayout(new FormLayout());
		
		ArrayList<String> oldJobs = new ArrayList<>();
		
		try
		{
			DatabaseManager dbManager = new DatabaseManager();
			dbManager.getConnection();
			oldJobs = dbManager.getOldJobs();
			dbManager.closeConnection();
		}
		catch (Exception e)
		{
			System.out.println("Exception inside experiment count fetch");
			e.printStackTrace();
		}
		
		GraphDisplayScreen graph = new GraphDisplayScreen();
		graph.showGraph(graphComposite, oldJobs);
		
		tbtmGraph.setControl(graphComposite);
	}
}
