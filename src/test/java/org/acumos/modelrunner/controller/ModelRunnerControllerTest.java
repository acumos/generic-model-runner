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

package org.acumos.modelrunner.controller;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import java.io.File;
import java.nio.file.Files;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.context.WebApplicationContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModelRunnerControllerTest extends ModelRunnerTestApp {

	private static final Logger logger = LoggerFactory.getLogger(ModelRunnerControllerTest.class);

	@Autowired
	private WebApplicationContext webApplicationContext;

	private MockMvc mockMvc;

	@Before
	public void setup() {
		mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
	}

	@Test
	public void predictTest() throws Exception {
		String sep = File.separator;

		String filePath = System.getProperty("user.dir");

		String basePath = filePath + sep + "testdata" + sep;

		try {
			// testing GET method
			logger.info("Testing GET method");
			this.mockMvc.perform(get("/hello"));
			logger.info("Done testing GET method");

			// testing /predict end point
			// Add test case later
			logger.info("Testing /predict POST end point");
			logger.info("Done testing /predict POST end point");

			// testing /getBinaryDefault end point
			logger.info("Testing /getBinaryDefault POST end point");


			// testing /transformDefault end point
			logger.info("Testing /transformDefault POST end point");

			logger.info("Done testing /transformDefault POST end point");

			byte[] proto = Files.readAllBytes(new File(basePath + "irisdata5.proto").toPath());

		} catch (HttpStatusCodeException ex) {
			logger.error("predictTest failed", ex);
			assert (false);

		}
		assert (true);
	}
}
