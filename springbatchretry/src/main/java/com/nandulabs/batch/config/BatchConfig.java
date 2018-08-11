package com.nandulabs.batch.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DeadlockLoserDataAccessException;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

import com.nandulabs.batch.listener.JobListener;
import com.nandulabs.batch.listener.Step1StepListener;
import com.nandulabs.batch.model.StudentDTO;
import com.nandulabs.batch.policy.CustomSkipPolicy;
import com.nandulabs.batch.reader.StaxEventItemReaderThreadSafe;
import com.nandulabs.batch.writer.EmbeddedDatabseJdbcBatchItemWriter;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private DataSource dataSource;

	@Bean
	public StaxEventItemReaderThreadSafe<StudentDTO> reader() throws Exception {
		StaxEventItemReaderThreadSafe<StudentDTO> reader = new StaxEventItemReaderThreadSafe<>();
		reader.setFragmentRootElementName("student");
		reader.setResource(new ClassPathResource("xml/student-error.xml"));

		Jaxb2Marshaller unmarshaller = new Jaxb2Marshaller();
		unmarshaller.setClassesToBeBound(StudentDTO.class);
		unmarshaller.afterPropertiesSet();

		reader.setUnmarshaller(unmarshaller);
		return reader;
	}

	@Bean
	public EmbeddedDatabseJdbcBatchItemWriter writer() {
		EmbeddedDatabseJdbcBatchItemWriter batchItemWriter = new EmbeddedDatabseJdbcBatchItemWriter();
		batchItemWriter.setDataSource(dataSource);
		batchItemWriter.setSql(
				"insert into records(name,emailAddress,purchasedPackage) values (:name,:emailAddress,:purchasedPackage)");
		batchItemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<StudentDTO>());
		return batchItemWriter;
	}

	@Bean
	public TaskExecutor taskExecutor() {
		SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
		return taskExecutor;
	}

	@Bean
	public Job job() throws Exception {
		return this.jobBuilderFactory.get("job")
				.incrementer(new RunIdIncrementer())
				.flow(step()).end()
				.listener(jobListener())
				.build();
	}

	@Bean
	public JobListener jobListener() {
		JobListener jobListener = new JobListener();
		jobListener.setDatasource(dataSource);
		return jobListener;
	}

	public Step step() throws Exception {
		return this.stepBuilderFactory.get("step")
				.<StudentDTO, StudentDTO>chunk(5)
				.reader(reader())
				.writer(writer())
				.faultTolerant()
				.skipPolicy(new CustomSkipPolicy())
				.retry(DeadlockLoserDataAccessException.class)
				.retryLimit(3)
				.taskExecutor(taskExecutor())
				.listener(stepListener())
				.throttleLimit(1)
				.build();
	}

	@Bean
	public Step1StepListener stepListener() {
		return new Step1StepListener();
	}
}
