package com.nandulabs.batch.listener;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

public class JobListener implements JobExecutionListener {
	
	private static final Logger log = LogManager.getLogger(JobListener.class);
	
	private DataSource datasource;
	private Connection connection;

	@Override
	public void beforeJob(JobExecution jobExecution) {
		log.info("START OF THE JOB");
		try {
			connection = datasource.getConnection();
		} catch (SQLException e) {
			log.error("EXCEPTION CONNECTION SQL : " + e);
		}
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		log.info("BDD CONTENT : ");
		
		String selectSQL = "SELECT COUNT(*) FROM records";
		try(PreparedStatement preparedStatement = connection.prepareStatement(selectSQL);) {
			
			ResultSet rs = preparedStatement.executeQuery();
			while (rs.next()) {
				log.info("NB RECORD IN DATABASE : " + rs.getInt(1));
			}
			
		} catch(Exception e) {
			log.error("ERROR QUERY SQL : " + e);
		}
		
		log.info("END OF THE JOB");
	}

	/**
	 * @return the datasource
	 */
	public DataSource getDatasource() {
		return datasource;
	}

	/**
	 * @param datasource the datasource to set
	 */
	public void setDatasource(DataSource datasource) {
		this.datasource = datasource;
	}

}
