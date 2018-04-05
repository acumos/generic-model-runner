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

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMultipartHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.request.RequestPostProcessor;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.context.WebApplicationContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModelRunnerControllerTest extends ModelRunnerTestApp {
	@Value("${plugin_root}")
	private String pluginRoot;

	private static final Logger logger = LoggerFactory.getLogger(ModelRunnerControllerTest.class);
	private static final String PROJECTROOT = System.getProperty("user.dir");
	private static final String SEP = File.separator;

	@Autowired
	private WebApplicationContext webApplicationContext;
	private MockMvc mockMvc;

	@Before
	public void setup() {
		mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
	}

	@Test
	public void predictTest() throws Exception {
		String testPath = new String(pluginRoot + SEP + "test");

		// Make sure plugin Root directories exist. If not, create them.
		File dir = new File(pluginRoot);
		boolean created = false;
		if (!dir.exists()) {
			created = dir.mkdirs();
			logger.info("predictTest: Creating pluginRoot directory: " + pluginRoot);
			logger.info("predictTest: Created pluginRoot directory? " + created);
		}
		File testDir = new File(testPath);
		if (!testDir.exists()) {
			created = testDir.mkdirs();
			logger.info("Creating JUnit test directory: " + testPath);
			logger.info("Created JUnit test directory ? " + created);

		}
		File modelSource = new File(PROJECTROOT + SEP + "models" + SEP + "model.template");
		File modelDest = new File(testPath + SEP + "SimpleMockModel.java");
		modelDest.setWritable(true, false);
		if (modelDest.createNewFile())
			logger.info("Creating an empty file");
		else
			logger.info("Unable to create an empty file");
		com.google.common.io.Files.copy(modelSource, modelDest);

		generateSimpleModel();

		try {
			// testing GET method
			logger.info("Testing GET method");
			this.mockMvc.perform(get("/hello"));
			logger.info("Done testing GET method");

			logger.info("Testing /putProto PUT method");
			byte[] protobytes = Files.readAllBytes(Paths.get("src", "test", "resources", "default.proto"));
			MockMultipartFile proto = new MockMultipartFile("proto", "default.proto", null, protobytes); 

			/*
			 * 
			 * https://stackoverflow.com/questions/38571716/how-to-put-multipart-form-data-
			 * using-spring-mockmvc/40656484
			 * 
			 */
			MockMultipartHttpServletRequestBuilder builder1 = MockMvcRequestBuilders.fileUpload("/putProto");
			builder1.with(new RequestPostProcessor() {
				@Override
				public MockHttpServletRequest postProcessRequest(MockHttpServletRequest request) {
					request.setMethod("PUT");
					return request;
				}
			});
			mockMvc.perform(builder1.file(proto)).andExpect(status().isOk());

			logger.info("Done testing /putProto PUT end point"); 

			logger.info("Testing /putModel PUT method");
			byte[] modelbytes = Files.readAllBytes(new File(testPath + SEP + "model.jar").toPath());
			MockMultipartFile modelFile = new MockMultipartFile("model", "model.jar", null, modelbytes);
			MockMultipartHttpServletRequestBuilder builder2 = MockMvcRequestBuilders.fileUpload("/putModel");
			builder2.with(new RequestPostProcessor() {
				@Override
				public MockHttpServletRequest postProcessRequest(MockHttpServletRequest request) {
					request.setMethod("PUT");
					return request;
				}
			});
			mockMvc.perform(builder2.file(modelFile)).andExpect(status().isOk());

			logger.info("Done testing /putModel PUT end point");

			logger.info("Testing /putModelConfig PUT method");

			byte[] configbytes0 = Files.readAllBytes(Paths.get("src", "test", "resources", "testConfig.properties"));
			MockMultipartFile modelConfig = new MockMultipartFile("modelConfig", "testConfig.properties", null,
					configbytes0);
			MockMultipartHttpServletRequestBuilder builder3 = MockMvcRequestBuilders.fileUpload("/putModelConfig");
			builder3.with(new RequestPostProcessor() {
				@Override
				public MockHttpServletRequest postProcessRequest(MockHttpServletRequest request) {
					request.setMethod("PUT");
					return request;
				}
			});
			mockMvc.perform(builder3.file(modelConfig)).andExpect(status().isOk());

			logger.info("Done testing /putModelConfig PUT end point"); 

			// testing /transform end point

			logger.info("Testing /transform POST end point");

			MockMultipartFile testCSV = new MockMultipartFile("csvFile", "testTransform.csv", "text/csv",
					"operand\n2\n7\n8\n".getBytes());
			byte[] modelbytes0 = Files.readAllBytes(new File(testPath + SEP + "model.jar").toPath());
			byte[] protobytes0 = Files.readAllBytes(Paths.get("src", "test", "resources", "simplemodel.proto"));
			MockMultipartFile testProto = new MockMultipartFile("proto", "simplemodel.proto", null, protobytes0);
			MockMultipartFile testModel = new MockMultipartFile("model", "model.jar", null, modelbytes0);
			MockHttpServletRequestBuilder builder0 = MockMvcRequestBuilders.fileUpload("/transform").file(testCSV)
					.file(testModel).file(testProto).param("operation", "multiply");

			byte[] resultsTransform0 = this.mockMvc.perform(builder0).andExpect(status().isOk()).andReturn()
					.getResponse().getContentAsByteArray();

			logger.info("Done testing /transform POST end point: " + resultsTransform0);

			// testing /transformDefault end point
			logger.info("Testing /transformDefault POST end point");

			MockMultipartFile f1 = new MockMultipartFile("csvFile", "testTransform.csv", "text/csv",
					"multiplier_data\n3333\n5\n8\n".getBytes());
			MockHttpServletRequestBuilder builder = MockMvcRequestBuilders.fileUpload("/transformDefault").file(f1)
					.param("operation", "multiply");

			byte[] resultsTransform = this.mockMvc.perform(builder).andExpect(status().isOk()).andReturn().getResponse()
					.getContentAsByteArray();
			logger.info("Done testing /transformDefault POST end point: " + resultsTransform);

			logger.info("Testing /getBinary POST method");

			// first change configuration properties to use multiplyAll method
			byte[] configbytes1 = Files.readAllBytes(
					new File(this.getClass().getResource("/sampleConfig1.properties").getFile()).toPath());
			MockMultipartFile sampleConfig1 = new MockMultipartFile("modelConfig", "sampleConfig1.properties", null,
					configbytes1);
			MockMultipartHttpServletRequestBuilder builder4 = MockMvcRequestBuilders.fileUpload("/putModelConfig");
			builder4.with(new RequestPostProcessor() {
				@Override
				public MockHttpServletRequest postProcessRequest(MockHttpServletRequest request) {
					request.setMethod("PUT");
					return request;
				}
			});
			mockMvc.perform(builder4.file(sampleConfig1)).andExpect(status().isOk());

			byte[] csvbytes1 = Files
					.readAllBytes(new File(this.getClass().getResource("/sample1.csv").getFile()).toPath());
			MockMultipartFile fb = new MockMultipartFile("csvFile", "sample1.csv", "text/csv", csvbytes1);

			byte[] protobytes1 = Files
					.readAllBytes(new File(this.getClass().getResource("/sample1.proto").getFile()).toPath());
			MockMultipartFile proto1 = new MockMultipartFile("proto", "sample1.proto", null, protobytes1);
			MockHttpServletRequestBuilder builderGB = MockMvcRequestBuilders.fileUpload("/getBinary").file(fb)
					.file(proto1);

			byte[] resultsGB = this.mockMvc.perform(builderGB.param("operation", "transform"))
					.andExpect(status().isOk()).andReturn().getResponse().getContentAsByteArray();

			logger.info("Done testing /getBinary POST end point: " + Arrays.toString(resultsGB));

			// testing /getBinaryDefault end point
			logger.info("Testing /getBinaryDefault POST end point");

			// Use sampleConfig2.properties which calls the transform method of the silly
			// model
			byte[] configbytes2 = Files.readAllBytes(
					new File(this.getClass().getResource("/sampleConfig2.properties").getFile()).toPath());
			MockMultipartFile sampleConfig2 = new MockMultipartFile("modelConfig", "sampleConfig2.properties", null,
					configbytes2);
			MockMultipartHttpServletRequestBuilder builder5 = MockMvcRequestBuilders.fileUpload("/putModelConfig");
			builder5.with(new RequestPostProcessor() {
				@Override
				public MockHttpServletRequest postProcessRequest(MockHttpServletRequest request) {
					request.setMethod("PUT");
					return request;
				}
			});
			mockMvc.perform(builder5.file(sampleConfig2)).andExpect(status().isOk());

			// Use sample2.proto
			byte[] protobytes2 = Files.readAllBytes(Paths.get("src", "test", "resources", "sample2.proto"));
			MockMultipartFile proto2 = new MockMultipartFile("proto", "sample2.proto", null, protobytes2);

			MockMultipartHttpServletRequestBuilder builder6 = MockMvcRequestBuilders.fileUpload("/putProto");
			builder6.with(new RequestPostProcessor() {
				@Override
				public MockHttpServletRequest postProcessRequest(MockHttpServletRequest request) {
					request.setMethod("PUT");
					return request;
				}
			});
			mockMvc.perform(builder6.file(proto2)).andExpect(status().isOk());
			
			// Use sample2.csv to test
			byte[] csvbytes2 = Files
					.readAllBytes(new File(this.getClass().getResource("/sample2.csv").getFile()).toPath());
			MockMultipartFile csvfile2 = new MockMultipartFile("csvFile", "sample2.csv", "text/csv", csvbytes2);
			byte[] resultsBinaryDefault = this.mockMvc.perform(MockMvcRequestBuilders.fileUpload("/getBinaryDefault")
					.file(csvfile2).param("operation", "transform")).andExpect(status().isOk()).andReturn()
					.getResponse().getContentAsByteArray();
			logger.info("Done testing POST /getBinaryDefault end point " + Arrays.toString(resultsBinaryDefault));

			// testing /operation/{operation} end point, use transform in this case
			logger.info("Testing /operation/{operation} POST end point");
			byte[] resultsPredict = this.mockMvc
					.perform(post("/operation/transform").contentType(MediaType.TEXT_PLAIN)
							.content(resultsBinaryDefault))
					.andExpect(status().isOk()).andReturn().getResponse().getContentAsByteArray();
			logger.info("Done testing POST /operation/{operation} end point " + Arrays.toString(resultsPredict));

			
			// Use sampleConfig3.properties which calls the classify method of the silly
			// model
			byte[] configbytes3 = Files.readAllBytes(
					new File(this.getClass().getResource("/sampleConfig3.properties").getFile()).toPath());
			MockMultipartFile sampleConfig3 = new MockMultipartFile("modelConfig", "sampleConfig3.properties", null,
					configbytes3);
			MockMultipartHttpServletRequestBuilder builder7 = MockMvcRequestBuilders.fileUpload("/putModelConfig");
			builder7.with(new RequestPostProcessor() {
				@Override
				public MockHttpServletRequest postProcessRequest(MockHttpServletRequest request) {
					request.setMethod("PUT");
					return request;
				}
			});
			mockMvc.perform(builder5.file(sampleConfig3)).andExpect(status().isOk());

			// Use sample3.proto
			byte[] protobytes3 = Files.readAllBytes(Paths.get("src", "test", "resources", "sample3.proto"));
			MockMultipartFile proto3 = new MockMultipartFile("proto", "sample3.proto", null, protobytes3);

			MockMultipartHttpServletRequestBuilder builder8 = MockMvcRequestBuilders.fileUpload("/putProto");
			builder8.with(new RequestPostProcessor() {
				@Override
				public MockHttpServletRequest postProcessRequest(MockHttpServletRequest request) {
					request.setMethod("PUT");
					return request;
				}
			});
			mockMvc.perform(builder8.file(proto3)).andExpect(status().isOk());

			// Use sample3.csv to test
			byte[] csvbytes3 = Files
					.readAllBytes(new File(this.getClass().getResource("/sample3.csv").getFile()).toPath());
			MockMultipartFile csvfile3 = new MockMultipartFile("csvFile", "sample3.csv", "text/csv", csvbytes3);

			byte[] resultsBinaryDefault1 = this.mockMvc.perform(MockMvcRequestBuilders.fileUpload("/getBinaryDefault")
					.file(csvfile3).param("operation", "classify")).andExpect(status().isOk()).andReturn().getResponse()
					.getContentAsByteArray();

			// testing /operation/{operation} end point, use classify in this case
			logger.info("Testing /operation/classify POST end point");
			byte[] resultsPredict1 = this.mockMvc
					.perform(post("/operation/classify").contentType(MediaType.TEXT_PLAIN)
							.content(resultsBinaryDefault1))
					.andExpect(status().isOk()).andReturn().getResponse().getContentAsByteArray();
			logger.info("Done testing POST /operation/classify end point " + Arrays.toString(resultsPredict1));

			
			// Use sampleConfig3_2.properties which calls the classify method of the silly
			// model
			byte[] configbytes3_2 = Files.readAllBytes(
					new File(this.getClass().getResource("/sampleConfig3_2.properties").getFile()).toPath());
			MockMultipartFile sampleConfig3_2 = new MockMultipartFile("modelConfig", "sampleConfig3_2.properties", null,
					configbytes3_2);
			MockMultipartHttpServletRequestBuilder builder9 = MockMvcRequestBuilders.fileUpload("/putModelConfig");
			builder9.with(new RequestPostProcessor() {
				@Override
				public MockHttpServletRequest postProcessRequest(MockHttpServletRequest request) {
					request.setMethod("PUT");
					return request;
				}
			});
			mockMvc.perform(builder9.file(sampleConfig3_2)).andExpect(status().isOk());

			// Use sample3_2.csv to test
			byte[] csvbytes4 = Files
					.readAllBytes(new File(this.getClass().getResource("/sample3_2.csv").getFile()).toPath());
			MockMultipartFile csvfile4 = new MockMultipartFile("csvFile", "sample3_2.csv", "text/csv", csvbytes4);
			byte[] resultsBinaryDefault2 = this.mockMvc.perform(MockMvcRequestBuilders.fileUpload("/getBinaryDefault")
					.file(csvfile4).param("operation", "aggregate")).andExpect(status().isOk()).andReturn()
					.getResponse().getContentAsByteArray();

			// testing /operation/aggregate end point
			logger.info("Testing /operation/aggregate POST end point");
			byte[] resultsPredict2 = this.mockMvc
					.perform(post("/operation/aggregate").contentType(MediaType.TEXT_PLAIN)
							.content(resultsBinaryDefault2))
					.andExpect(status().isOk()).andReturn().getResponse().getContentAsByteArray();
			logger.info("Done testing POST /operation/aggregate end point " + Arrays.toString(resultsPredict2));
			
			// Use sample4.proto
			byte[] protobytes4 = Files.readAllBytes(Paths.get("src", "test", "resources", "sample4.proto"));
			MockMultipartFile proto4 = new MockMultipartFile("proto", "sample4.proto", null, protobytes4);

			MockMultipartHttpServletRequestBuilder builder10 = MockMvcRequestBuilders.fileUpload("/putProto");
			builder10.with(new RequestPostProcessor() {
				@Override
				public MockHttpServletRequest postProcessRequest(MockHttpServletRequest request) {
					request.setMethod("PUT");
					return request;
				}
			});
			mockMvc.perform(builder10.file(proto4)).andExpect(status().isOk());
			
			// upload sampleConfig4.properties
			byte[] configbytes4 = Files.readAllBytes(Paths.get("src", "test", "resources", "sampleConfig4.properties"));
			MockMultipartFile sampleConfig4 = new MockMultipartFile("modelConfig", "sampleConfig4.properties", null,
					configbytes4);
			MockMultipartHttpServletRequestBuilder builderModelConfig4 = MockMvcRequestBuilders.fileUpload("/putModelConfig");
			builderModelConfig4.with(new RequestPostProcessor() {
				@Override
				public MockHttpServletRequest postProcessRequest(MockHttpServletRequest request) {
					request.setMethod("PUT");
					return request;
				}
			});
			mockMvc.perform(builderModelConfig4.file(sampleConfig4)).andExpect(status().isOk());

			// use sample4.csv test
			byte[] csvSample4 = Files.readAllBytes(new File(this.getClass().getResource("/sample4.csv").getFile()).toPath());
			MockMultipartFile csvMock4 = new MockMultipartFile("csvFile", "sample4.csv", "text/csv", csvSample4);
			byte[] resultsBinaryDefault4 = this.mockMvc.perform(MockMvcRequestBuilders.fileUpload("/getBinaryDefault")
					.file(csvMock4).param("operation", "testEnum")).andExpect(status().isOk()).andReturn()
					.getResponse().getContentAsByteArray();

			// testing /operation/testEnum end point
			logger.info("Testing /operation/testEnum POST end point");
			byte[] resultsPredict4 = this.mockMvc
					.perform(post("/operation/testEnum").contentType(MediaType.TEXT_PLAIN)
							.content(resultsBinaryDefault4))
					.andExpect(status().isOk()).andReturn().getResponse().getContentAsByteArray();
			logger.info("Done testing POST /operation/testEnum end point " + Arrays.toString(resultsPredict4));
			

		} catch (HttpStatusCodeException ex) {
			logger.error("predictTest failed", ex);
			assert (false);
		}
		assert (true);
	}

	/**
	 * 
	 * Generate simple model.jar in the test directory for loading
	 */
	private void generateSimpleModel() {
		int exitVal = -1;

		try {
			String simpleModelPath = new String(pluginRoot + SEP + "test");
			// find the $PATH
			ProcessBuilder	pb0 = new ProcessBuilder("which", "javac");
			pb0.directory(new File(simpleModelPath));

			logger.info("generateSimpleModel: executing command: \"which javac\"");
			Process p0 = pb0.start();
			// get the error stream of the process and print it
			InputStream error0 = p0.getErrorStream();
			printCmdError(error0);
			PrintWriter printWriter0 = new PrintWriter(p0.getOutputStream());
			BufferedReader bufferedReader0 = new BufferedReader(new InputStreamReader(p0.getInputStream()));
			printCmdOutput(bufferedReader0);

			printWriter0.flush();
			exitVal = p0.waitFor();

			logger.info("generateSimpleModel: Exit Value for which javac cmd: " + exitVal);

			// Just to find where jar program is
			pb0 = new ProcessBuilder("which", "jar");
			pb0.directory(new File(simpleModelPath));
			logger.info("generateSimpleModel: executing command: \"which jar\"");

			p0 = pb0.start();
			// get the error stream of the process and print it
			error0 = p0.getErrorStream();
			printCmdError(error0);
			printWriter0 = new PrintWriter(p0.getOutputStream());
			bufferedReader0 = new BufferedReader(new InputStreamReader(p0.getInputStream()));
			printCmdOutput(bufferedReader0);
			printWriter0.flush();
			exitVal = p0.waitFor();

			logger.info("generateSimpleModel: Exit Value for which jar cmd: " + exitVal);
			// Done find $PATH
						
			ProcessBuilder pb = new ProcessBuilder("javac", "SimpleMockModel.java");
			pb.directory(new File(simpleModelPath));

			logger.info("generateSimpleModel: Setting directory to : " + simpleModelPath
					+ " before building simple model class file");
			logger.info("generateSimpleModel: executing command: \"javac SimpleMockModel.java\" from directory "
					+ simpleModelPath);
			Process p = pb.start();
			// get the error stream of the process and print it
			InputStream error = p.getErrorStream();
			printCmdError(error);
			
			PrintWriter printWriter = new PrintWriter(p.getOutputStream());
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(p.getInputStream()));
			printCmdOutput(bufferedReader);
			bufferedReader.close();
			printWriter.flush();
			exitVal = p.waitFor();

			logger.info("generateSimpleModel: Exit Value for javac cmd: " + exitVal);

			pb = new ProcessBuilder("jar", "-cvf", "model.jar", "SimpleMockModel.class");
			pb.directory(new File(simpleModelPath));

			logger.info("generateSimpleModel: Setting directory to : " + simpleModelPath
					+ " before producing simple model.jar");
			logger.info(
					"generateSimpleModel: executing command: \"jar -cvf model.jar SimpleMockModel.class\" from directory "
							+ simpleModelPath);
			p = pb.start();
			// get the error stream of the process and print it
			error = p.getErrorStream();
			for (int i = 0; i < error.available(); i++) {
				logger.error("" + error.read());
			}

			printWriter = new PrintWriter(p.getOutputStream());
			bufferedReader = new BufferedReader(new InputStreamReader(p.getInputStream()));
			printCmdOutput(bufferedReader);

			printWriter.flush();
			exitVal = p.waitFor();

			logger.info("generateSimpleModel: Exit Value for jar cmd: " + exitVal);
		} catch (Exception ex) {
			logger.error("generateSimpleModel: Failed producing simple model.jar ", ex);
			return;
		}
		if (exitVal != 0)
			logger.error("generateSimpleModel: Failed producing simple model.jar");
		else
			logger.info("generateSimpleModel: Completed producing simple model.jar!");

	}

	/**
	 * Print out contents of BufferedReader
	 * 
	 * @param bufferedReader
	 * @throws IOException
	 */
	private void printCmdOutput(BufferedReader bufferedReader) throws IOException {
		String currentLine;

		while ((currentLine = bufferedReader.readLine()) != null) {
			logger.info("printCmdOuput: " + currentLine);
		}
		bufferedReader.close();
	}
	
	/*
	 * print out command error message
	 */
	private void printCmdError(InputStream error) throws IOException {
		for (int i = 0; i < error.available(); i++) {
			logger.error("printCmdError: " + error.read());
		}
		error.close();
	}
}
