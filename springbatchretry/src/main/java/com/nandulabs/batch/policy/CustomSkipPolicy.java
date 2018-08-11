package com.nandulabs.batch.policy;

import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DataIntegrityViolationException;

public class CustomSkipPolicy implements SkipPolicy {

	@Override
	public boolean shouldSkip(Throwable t, int skipCount) throws SkipLimitExceededException {
		
		if(t instanceof DataAccessResourceFailureException) {
			System.out.println("Custom skip policy parse error "+ t.getLocalizedMessage());
			return true;
		} else if(t instanceof DataIntegrityViolationException) {
			System.out.println("Custom skip policy SQL error "+ t.getLocalizedMessage());
		} else {
			System.out.println("Custom skip policy Deadlock error "+ t.getLocalizedMessage());
		}
		
		return true;
	}

}
