package com.nandulabs.batch.listener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

public class Step1StepListener implements StepExecutionListener {
	
	private static final Logger log = LogManager.getLogger(Step1StepListener.class);

	@Override
	public void beforeStep(StepExecution arg0) {
		log.info("Before Step Execution....!!!");
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		log.info(stepExecution.getSummary());
		log.info("After Step Execution....!!!");
		return null;
	}

}
