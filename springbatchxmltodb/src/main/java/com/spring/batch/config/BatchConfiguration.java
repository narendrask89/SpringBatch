package com.spring.batch.config;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.oxm.xstream.XStreamMarshaller;

import com.spring.batch.model.Person;
import com.spring.batch.processor.PersonItemProcessor;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private DataSource dataSource;

	@Bean
	public StaxEventItemReader<Person> reader() {
		StaxEventItemReader<Person> reader = new StaxEventItemReader<Person>();
		reader.setResource(new ClassPathResource("person.xml"));
		reader.setFragmentRootElementName("person");

		Map<String, String> aliasMap = new HashMap<>();
		aliasMap.put("person", "com.spring.batch.model.Person");
		XStreamMarshaller unmarshaller = new XStreamMarshaller();
		unmarshaller.setAliases(aliasMap);
		
		reader.setUnmarshaller(unmarshaller);
		return reader;
	}

	@Bean
	public PersonItemProcessor processor() {
		return new PersonItemProcessor();
	}
	@Bean
	public JdbcBatchItemWriter<Person> writer() {
		JdbcBatchItemWriter<Person> batchItemWriter = new JdbcBatchItemWriter<>();
		batchItemWriter.setSql("INSERT INTO person(person_id,first_name,last_name,email,age) VALUES(?,?,?,?,?)");
		batchItemWriter.setItemPreparedStatementSetter(new PersonItemPreparedStatementSetter());
		batchItemWriter.setDataSource(dataSource);
		batchItemWriter.afterPropertiesSet();
		return batchItemWriter;
	}
	
	@Bean
    public Job importUserJob() {
        return jobBuilderFactory.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .flow(step1())
                .end()
                .build();
    }
	
	@Bean
	public Step step1() {
		return this.stepBuilderFactory.get("step1")
				.<Person,Person>chunk(100)
				.reader(reader())
				.processor(processor())
				.writer(writer())
				.build();
	}
}
