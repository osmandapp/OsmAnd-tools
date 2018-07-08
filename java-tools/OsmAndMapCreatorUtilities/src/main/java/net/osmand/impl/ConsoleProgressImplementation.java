package net.osmand.impl;

import java.text.MessageFormat;

import net.osmand.IProgress;
import net.osmand.PlatformUtil;
import net.osmand.util.Algorithms;

import org.apache.commons.logging.Log;


public class ConsoleProgressImplementation implements IProgress {
	public static double deltaPercentsToPrint = 3.5;
	public static long deltaTimeToPrint = 1000;
	public static long deltaTimeToPrintMax = 120000;
	private static Log log = PlatformUtil.getLog(ConsoleProgressImplementation.class);

	protected String currentTask;
	protected long work;
	protected long currentDone;
	protected double delta;
	protected long previousTaskStarted = 0;
	protected long lastTimePrinted = 0;

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

	@Override
	public void finishTask() {
		log.info("Task " + currentTask + " is finished "); //$NON-NLS-1$ //$NON-NLS-2$
		this.currentTask = null;

	}

	@Override
	public boolean isIndeterminate() {
		return work == -1;
	}

	@Override
	public void progress(int deltaWork) {
		this.currentDone += deltaWork;
		printIfNeeded();
	}

	protected  void printIfNeeded() {
		long now = System.currentTimeMillis();
		if (getCurrentPercent() - lastPercentPrint >= delta || 
				now - lastTimePrinted  > deltaTimeToPrintMax) {
			this.lastPercentPrint = getCurrentPercent();
			if(now - lastTimePrinted >= deltaTimeToPrint){
				log.info(getPrintMessage()); //$NON-NLS-1$
				lastTimePrinted = now;
			}

		}
	}

	protected String getPrintMessage() {
		return MessageFormat.format("Done {0} %.", getCurrentPercent());
	}

	public double getCurrentPercent(){
		return (double) currentDone * 100.0d / work;
	}

	@Override
	public void remaining(int remainingWork) {
		this.currentDone = work - remainingWork;
		printIfNeeded();
	}
	
	public void remainingLong(long remainingWork) {
		this.currentDone = work - remainingWork;
		printIfNeeded();
	}

	@Override
	public void startTask(String taskName, int work) {
		startTaskLong(taskName, work);
	}
	
	
	public void startTaskLong(String taskName, long work) {
		if(!Algorithms.objectEquals(currentTask, taskName)){
			this.currentTask = taskName;
			log.debug("Memory before task exec: " + Runtime.getRuntime().totalMemory() + " free : " + Runtime.getRuntime().freeMemory()); //$NON-NLS-1$ //$NON-NLS-2$
			if (previousTaskStarted == 0) {
				log.info(taskName + " started - " + work); //$NON-NLS-1$
			} else {
				log.info(taskName + " started after " + (System.currentTimeMillis() - previousTaskStarted) + " ms" + " - " + work); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
			}
			previousTaskStarted = System.currentTimeMillis();
		}
		startWorkLong(work);
	}

	@Override
	public void startWork(int work) {
		startWorkLong(work);
	}

	private void startWorkLong(long work) {
		if(this.work != work){
			this.work = work;
			log.info("Task " + currentTask + ": work total has changed to " + work); //$NON-NLS-1$
		}
		this.currentDone = 0;
		this.lastPercentPrint = 0;
	}

	@Override
	public boolean isInterrupted() {
		return false;
	}

	@Override
	public void setGeneralProgress(String genProgress) {
		// not implemented now
	}


}
