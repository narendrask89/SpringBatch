package com.nandulabs.batch.writer;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.dao.DeadlockLoserDataAccessException;

import com.nandulabs.batch.model.StudentDTO;

public class EmbeddedDatabseJdbcBatchItemWriter extends JdbcBatchItemWriter<StudentDTO> {

	private static final Logger log = LogManager.getLogger(EmbeddedDatabseJdbcBatchItemWriter.class);

	@Override
	public void write(List<? extends StudentDTO> listOfReport) throws Exception {
		log.info("Total item received " + listOfReport.size());
		try {
			super.write(listOfReport);
		} catch (Exception e) {
			throw new DeadlockLoserDataAccessException("connection problem", e);
		}
		log.info(listOfReport.size() + " items written !");
	}

}
