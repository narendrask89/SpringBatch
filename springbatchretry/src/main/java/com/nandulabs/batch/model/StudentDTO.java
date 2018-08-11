package com.nandulabs.batch.model;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang3.builder.ToStringBuilder;

@XmlRootElement(name = "student")
public class StudentDTO {

	private String emailAddress;
	private String name;
	private String purchasedPackage;

	public StudentDTO() {
	}

	public String getEmailAddress() {
		return emailAddress;
	}

	public String getName() {
		return name;
	}

	public String getPurchasedPackage() {
		return purchasedPackage;
	}

	public void setEmailAddress(String emailAddress) {
		this.emailAddress = emailAddress;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setPurchasedPackage(String purchasedPackage) {
		this.purchasedPackage = purchasedPackage;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("emailAddress", this.emailAddress).append("name", this.name)
				.append("purchasedPackage", this.purchasedPackage).toString();
	}
}