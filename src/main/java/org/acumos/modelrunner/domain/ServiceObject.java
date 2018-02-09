package org.acumos.modelrunner.domain;

/**
 * Service Object that holds fields inside service structure of proto file
 */
public class ServiceObject{
	String service;
	String inputClass;
	String outputClass;

	public ServiceObject(String service, String inputClass, String outputClass) {
		super();
		this.service = service;
		this.inputClass = inputClass;
		this.outputClass = outputClass;
	}

	public String getService() {
		return service;
	}

	public void setService(String service) {
		this.service = service;
	}

	public String getInputClass() {
		return inputClass;
	}

	public void setInputClass(String inputClass) {
		this.inputClass = inputClass;
	}

	public String getOutputClass() {
		return outputClass;
	}

	public void setOutputClass(String outputClass) {
		this.outputClass = outputClass;
	}

	@Override
	public String toString() {
		return "ServiceObjec [service=" + service + ", inputClass=" + inputClass + ", outputClass=" + outputClass + "]";
	}

}
