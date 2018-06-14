package com.spring.batch.config;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.springframework.batch.item.database.ItemPreparedStatementSetter;

import com.spring.batch.model.Person;

public class PersonDetailItemPreparedStatementSetter implements ItemPreparedStatementSetter<Person> {

	@Override
	public void setValues(Person person, PreparedStatement ps) throws SQLException {
		ps.setString(1, person.getEmail());
		ps.setInt(2, person.getAge());
	}

}
