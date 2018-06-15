package net.osmand.impl;

import java.text.MessageFormat;

import net.osmand.PlatformUtil;

import org.apache.commons.logging.Log;


public class ConsoleProgressImplementation  {
	public static double deltaPercentsToPrint = 3.5;
	public static long deltaTimeToPrint = 1000;
	private static Log log = PlatformUtil.getLog(ConsoleProgressImplementation.class);
	
	String currentTask;
	int work;
	int currentDone;
	double delta;
	long deltaTime = deltaTimeToPrint;
	private long previousTaskStarted = 0;
	private long lastTimePrinted = 0;
	
	double lastPercentPrint = 0;
	public ConsoleProgressImplementation(){
		delta = deltaPercentsToPrint;
	}
	
	public ConsoleProgressImplementation(double deltaToPrint){
		delta = deltaToPrint;
	}
	
	public ConsoleProgressImplementation(double deltaToPrint, int deltaTime){
		delta = deltaToPrint;
		deltaToPrint = deltaTime;
	}
	
	public void finishTask() {
		log.info("Task " + currentTask + " is finished "); //$NON-NLS-1$ //$NON-NLS-2$
		this.currentTask = null;
		
	}

	public boolean isIndeterminate() {
		return work == -1;
	}

	public void progress(int deltaWork) {
		this.currentDone += deltaWork;
		printIfNeeded();
	}
	
	private void printIfNeeded() {
		if(getCurrentPercent() - lastPercentPrint >= delta){
			this.lastPercentPrint = getCurrentPercent();
			long now = System.currentTimeMillis();
			if(now - lastTimePrinted >= deltaTimeToPrint || deltaTime < 0){
				log.info(MessageFormat.format("Done {0} %.", getCurrentPercent())); //$NON-NLS-1$
				lastTimePrinted = now;
			}
			
		}
	}

	public double getCurrentPercent(){
		return (double) currentDone * 100.0d / work;
	}

	public void remaining(int remainingWork) {
		this.currentDone = work - remainingWork;
		printIfNeeded();
	}

	public void startTask(String taskName, int work) {
		if(currentTask == null || !currentTask.equals(taskName)){
			this.currentTask = taskName;
			log.debug("Memory before task exec: " + Runtime.getRuntime().totalMemory() + " free : " + Runtime.getRuntime().freeMemory()); //$NON-NLS-1$ //$NON-NLS-2$
			if (previousTaskStarted == 0) {
				log.info(taskName + " started - " + work); //$NON-NLS-1$
			} else {
				log.info(taskName + " started after " + (System.currentTimeMillis() - previousTaskStarted) + " ms" + " - " + work); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			}
			previousTaskStarted = System.currentTimeMillis();
		}
		startWork(work);
	}

	public void startWork(int work) {
		if(this.work != work){
			this.work = work;
			log.info("Task " + currentTask + ": work total has changed to " + work); //$NON-NLS-1$
		}
		this.currentDone = 0;
		this.lastPercentPrint = 0;
	}

	public boolean isInterrupted() {
		return false;
	}


}
