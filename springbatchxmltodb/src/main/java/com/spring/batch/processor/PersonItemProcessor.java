package com.spring.batch.processor;

import org.springframework.batch.item.ItemProcessor;

import com.spring.batch.model.Person;

public class PersonItemProcessor implements ItemProcessor<Person, Person> {

	@Override
	public Person process(Person item) throws Exception {
		return item;
	}

}
