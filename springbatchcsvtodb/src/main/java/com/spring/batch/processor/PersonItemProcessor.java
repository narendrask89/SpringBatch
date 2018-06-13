package com.spring.batch.processor;

import org.springframework.batch.item.ItemProcessor;

import com.spring.batch.model.Person;

public class PersonItemProcessor implements ItemProcessor<Person, Person> {

	@Override
	public Person process(Person item) throws Exception {

		Person person = new Person(item.getFirstName().toUpperCase(), item.getLastName().toUpperCase(), item.getEmail(),
				item.getAge());
		return person;
	}

}
