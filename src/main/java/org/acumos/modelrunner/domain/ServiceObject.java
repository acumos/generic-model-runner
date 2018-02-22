/*-
 * ===============LICENSE_START=======================================================
 * Acumos
 * ===================================================================================
 * Copyright (C) 2017 AT&T Intellectual Property & Tech Mahindra. All rights reserved.
 * ===================================================================================
 * This Acumos software file is distributed by AT&T and Tech Mahindra
 * under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ===============LICENSE_END=========================================================
 */

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
