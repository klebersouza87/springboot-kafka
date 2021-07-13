package com.example.kafkaandspringboot.dto;

public class Greeting {
	
	String message;
	String name;
	
	public Greeting() {}
	
	public Greeting(String message, String name) {
		super();
		this.message = message;
		this.name = name;
	}

	public String getMessage() {
		return message;
	}
	
	public void setMessage(String message) {
		this.message = message;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String toString() {
		return "Greeting [ name= " + name + ", message= " + message + " ]";
	}
	
}
