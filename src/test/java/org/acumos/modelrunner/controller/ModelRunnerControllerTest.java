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

import org.acumos.modelrunner.utils.ExecCmdUtil;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
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

import com.google.common.collect.ImmutableList;

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

			logger.info("Testing /proto PUT method");
			byte[] protobytes = Files.readAllBytes(Paths.get("src", "test", "resources", "default.proto"));
			MockMultipartFile proto = new MockMultipartFile("proto", "default.proto", null, protobytes);

			/*
			 * 
			 * https://stackoverflow.com/questions/38571716/how-to-put-multipart-form-data-
			 * using-spring-mockmvc/40656484
			 * 
			 */
			MockMultipartHttpServletRequestBuilder builder1 = MockMvcRequestBuilders.multipart("/proto");
			builder1.with(new RequestPostProcessor() {
				@Override
				public MockHttpServletRequest postProcessRequest(MockHttpServletRequest request) {
					request.setMethod("PUT");
					return request;
				}
			});
			mockMvc.perform(builder1.file(proto)).andExpect(status().isOk());

			logger.info("Done testing /proto PUT end point");

			logger.info("Testing /model PUT method");
			byte[] modelbytes = Files.readAllBytes(new File(testPath + SEP + "model.jar").toPath());
			MockMultipartFile modelFile = new MockMultipartFile("model", "model.jar", null, modelbytes);
			MockMultipartHttpServletRequestBuilder builder2 = MockMvcRequestBuilders.multipart("/model");
			builder2.with(new RequestPostProcessor() {
				@Override
				public MockHttpServletRequest postProcessRequest(MockHttpServletRequest request) {
					request.setMethod("PUT");
					return request;
				}
			});
			mockMvc.perform(builder2.file(modelFile)).andExpect(status().isOk());

			logger.info("Done testing /model PUT end point");

			logger.info("Testing /model/configuration PUT method");

			byte[] configbytes0 = Files.readAllBytes(Paths.get("src", "test", "resources", "testConfig.properties"));
			MockMultipartFile modelConfig = new MockMultipartFile("modelConfig", "testConfig.properties", null,
					configbytes0);
			MockMultipartHttpServletRequestBuilder builder3 = MockMvcRequestBuilders.multipart("/model/configuration");
			builder3.with(new RequestPostProcessor() {
				@Override
				public MockHttpServletRequest postProcessRequest(MockHttpServletRequest request) {
					request.setMethod("PUT");
					return request;
				}
			});
			mockMvc.perform(builder3.file(modelConfig)).andExpect(status().isOk());

			logger.info("Done testing /model/configuration PUT end point");

			// testing /transformCSV end point

			logger.info("Testing /transformCSV POST end point");

			MockMultipartFile testCSV = new MockMultipartFile("csvFile", "testTransform.csv", "text/csv",
					"operand\n2\n7\n8\n".getBytes());
			byte[] modelbytes0 = Files.readAllBytes(new File(testPath + SEP + "model.jar").toPath());
			/*
			 * https://stackoverflow.com/questions/28673651/how-to-get-the-path-of-src-test-
			 * resources-directory-in-junit
			 */
			byte[] protobytes0 = Files.readAllBytes(Paths.get("src", "test", "resources", "simplemodel.proto"));
			MockMultipartFile testProto = new MockMultipartFile("proto", "simplemodel.proto", null, protobytes0);
			MockMultipartFile testModel = new MockMultipartFile("model", "model.jar", null, modelbytes0);
			MockHttpServletRequestBuilder builder0 = MockMvcRequestBuilders.multipart("/transformCSV").file(testCSV)
					.file(testModel).file(testProto).param("operation", "multiply");

			byte[] resultsTransform0 = this.mockMvc.perform(builder0).andExpect(status().isOk()).andReturn()
					.getResponse().getContentAsByteArray();

			logger.info("Done testing /transformCSV POST end point: " + resultsTransform0);

			// testing /transformCSVDefault end point
			logger.info("Testing /transformCSVDefault POST end point");

			MockMultipartFile f1 = new MockMultipartFile("csvFile", "testTransform.csv", "text/csv",
					"multiplier_data\n3333\n5\n8\n".getBytes());
			MockHttpServletRequestBuilder builder = MockMvcRequestBuilders.multipart("/transformCSVDefault").file(f1)
					.param("operation", "multiply");

			byte[] resultsTransform = this.mockMvc.perform(builder).andExpect(status().isOk()).andReturn().getResponse()
					.getContentAsByteArray();
			logger.info("Done testing /transformCSVDefault POST end point: " + resultsTransform);

			////
			logger.info("Testing transformJSON POST end point");

			// first change configuration properties to use JSON inputs/outputs formats
			byte[] configbytesJson = Files.readAllBytes(
					new File(this.getClass().getResource("/sampleConfigJson.properties").getFile()).toPath());
			MockMultipartFile sampleConfigJson = new MockMultipartFile("modelConfig", "sampleConfigJson.properties",
					null, configbytesJson);
			MockMultipartHttpServletRequestBuilder builderConfigJson = MockMvcRequestBuilders
					.multipart("/model/configuration");
			builderConfigJson.with(new RequestPostProcessor() {
				@Override
				public MockHttpServletRequest postProcessRequest(MockHttpServletRequest request) {
					request.setMethod("PUT");
					return request;
				}
			});
			mockMvc.perform(builderConfigJson.file(sampleConfigJson)).andExpect(status().isOk());

			String jsonStr = "{\"rows\":[{\"operand\":2},{\"operand\":3},{\"operand\":4}]}";
			MockMultipartFile jsondata = new MockMultipartFile("jsonFile", "testRandom.json", "text/json",
					jsonStr.getBytes());

			byte[] protobytesJson0 = Files.readAllBytes(Paths.get("src", "test", "resources", "sampleJson0.proto"));
			MockMultipartFile protoJson0 = new MockMultipartFile("proto", "sampleJson0.proto", null, protobytesJson0);

			MockHttpServletRequestBuilder builderTransformJson = MockMvcRequestBuilders.multipart("/transformJSON")
					.file(jsondata).file(modelFile).file(protoJson0).param("operation", "testJSON");

			byte[] resultsTransformJson0 = this.mockMvc.perform(builderTransformJson).andExpect(status().isOk())
					.andReturn().getResponse().getContentAsByteArray();

			logger.info("Done testing transformJSON POST end point: " + resultsTransformJson0);

			// testing transformJSONDefault end point, using sampleJson0.proto

			MockMultipartHttpServletRequestBuilder builderProtoJson = MockMvcRequestBuilders.multipart("/proto");
			builderProtoJson.with(new RequestPostProcessor() {
				@Override
				public MockHttpServletRequest postProcessRequest(MockHttpServletRequest request) {
					request.setMethod("PUT");
					return request;
				}
			});
			mockMvc.perform(builderProtoJson.file(protoJson0)).andExpect(status().isOk());
			logger.info("Testing /transformJSONDefault POST end point");

			builderTransformJson = MockMvcRequestBuilders.multipart("/transformJSONDefault").file(jsondata)
					.param("operation", "testJSON");
			resultsTransformJson0 = this.mockMvc.perform(builderTransformJson).andExpect(status().isOk()).andReturn()
					.getResponse().getContentAsByteArray();
			logger.info("Done testing /transformJSONDefault POST end point: " + resultsTransformJson0);

			//// end of transformJSONDefault
			//// Starting of getBinaryJSON

			logger.info("Testing /getBinaryJSON POST method");

			String jsonStr1 = "{\"rows\":[{\"DOY\":166,\"LATITUDE\":49.047339,\"LONGITUDE\":-90.3977,\"pseudo_f1\":155525,\"pseudo_f2\":185.455,\"pseudo_f3\":\"Apple - iPhone7\",\"padding\":1},"
					+ "{\"DOY\":72,\"LATITUDE\":42.657982,\"LONGITUDE\":-81.153009,\"pseudo_f1\":1641166667,\"pseudo_f2\":30.679,\"pseudo_f3\":\"Apple - iPhone7\",\"padding\":2}],"
					+ "\"rows2\":[{\"subrow\":2,\"subrow2\":22222222,\"subrow3\":-2,\"subrow4\":-222222,\"subrow5\":555,\"subrow6\":5,\"overheads\":[true,true]},"
					+ "{\"subrow\":3,\"subrow2\":33333333,\"subrow3\":-3,\"subrow4\":-33333333,\"subrow5\":666,\"subrow6\":6,\"overheads\":[false,false,false]}]}";

			MockMultipartFile inpJsonfile = new MockMultipartFile("jsonFile", "testRandom1.json", "text/json",
					jsonStr1.getBytes());

			byte[] bytesTestJson = Files
					.readAllBytes(new File(this.getClass().getResource("/sampleJson1.proto").getFile()).toPath());
			MockMultipartFile protoTestJson = new MockMultipartFile("proto", "sampleJson1.proto", null, bytesTestJson);
			MockHttpServletRequestBuilder builderGBJ = MockMvcRequestBuilders.multipart("/getBinaryJSON")
					.file(inpJsonfile).file(protoTestJson);

			byte[] resultsBinJson = this.mockMvc.perform(builderGBJ.param("operation", "predict"))
					.andExpect(status().isOk()).andReturn().getResponse().getContentAsByteArray();

			logger.info("Done testing /getBinaryJSON POST end point: " + Arrays.toString(resultsBinJson));

			// testing /getBinaryJSONDefault end point
			logger.info("Testing /getBinaryJSONDefault POST end point");

			// Set default.proto to sampleJson1.proto
			MockMultipartHttpServletRequestBuilder builderJsonProto = MockMvcRequestBuilders.multipart("/proto");
			builderJsonProto.with(new RequestPostProcessor() {
				@Override
				public MockHttpServletRequest postProcessRequest(MockHttpServletRequest request) {
					request.setMethod("PUT");
					return request;
				}
			});
			mockMvc.perform(builderJsonProto.file(protoTestJson)).andExpect(status().isOk());

			byte[] resultsBinDefaultJson = this.mockMvc
					.perform(MockMvcRequestBuilders.multipart("/getBinaryJSONDefault").file(inpJsonfile)
							.param("operation", "predict"))
					.andExpect(status().isOk()).andReturn().getResponse().getContentAsByteArray();
			logger.info("Done testing POST /getBinaryJSONDefault end point " + Arrays.toString(resultsBinDefaultJson));

			// testing /{operation} end point, use predict in this case
			logger.info("Testing /{operation} POST end point");
			// first change configuration properties to use JSON inputs/outputs formats
			byte[] configbytesJson1 = Files.readAllBytes(
					new File(this.getClass().getResource("/sampleConfigJson1.properties").getFile()).toPath());
			MockMultipartFile sampleConfigJson1 = new MockMultipartFile("modelConfig", "sampleConfigJson1.properties",
					null, configbytesJson1);
			MockMultipartHttpServletRequestBuilder builderConfigJson1 = MockMvcRequestBuilders
					.multipart("/model/configuration");
			builderConfigJson1.with(new RequestPostProcessor() {
				@Override
				public MockHttpServletRequest postProcessRequest(MockHttpServletRequest request) {
					request.setMethod("PUT");
					return request;
				}
			});
			mockMvc.perform(builderConfigJson1.file(sampleConfigJson1)).andExpect(status().isOk());

			byte[] resultsPredictJSON = this.mockMvc
					.perform(post("/predict").contentType(MediaType.TEXT_PLAIN).content(resultsBinDefaultJson))
					.andExpect(status().isOk()).andReturn().getResponse().getContentAsByteArray();
			logger.info("Done testing POST /{operation} end point " + Arrays.toString(resultsPredictJSON));

			logger.info("Testing /getBinary POST method");
			// first change configuration properties to use multiplyAll method
			byte[] configbytes1 = Files.readAllBytes(
					new File(this.getClass().getResource("/sampleConfig1.properties").getFile()).toPath());
			MockMultipartFile sampleConfig1 = new MockMultipartFile("modelConfig", "sampleConfig1.properties", null,
					configbytes1);
			MockMultipartHttpServletRequestBuilder builder4 = MockMvcRequestBuilders.multipart("/model/configuration");
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
			MockHttpServletRequestBuilder builderGB = MockMvcRequestBuilders.multipart("/getBinary").file(fb)
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
			MockMultipartHttpServletRequestBuilder builder5 = MockMvcRequestBuilders.multipart("/model/configuration");
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

			MockMultipartHttpServletRequestBuilder builder6 = MockMvcRequestBuilders.multipart("/proto");
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
			byte[] resultsBinaryDefault = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/getBinaryDefault")
					.file(csvfile2).param("operation", "transform")).andExpect(status().isOk()).andReturn()
					.getResponse().getContentAsByteArray();
			logger.info("Done testing POST /getBinaryDefault end point " + Arrays.toString(resultsBinaryDefault));

			// testing /{operation} end point, use transform in this case
			logger.info("Testing /{operation} POST end point");
			byte[] resultsPredict = this.mockMvc
					.perform(post("/transform").contentType(MediaType.TEXT_PLAIN).content(resultsBinaryDefault))
					.andExpect(status().isOk()).andReturn().getResponse().getContentAsByteArray();
			logger.info("Done testing POST /{operation} end point " + Arrays.toString(resultsPredict));

			// Use sampleConfig3.properties which calls the classify method of the silly
			// model
			byte[] configbytes3 = Files.readAllBytes(
					new File(this.getClass().getResource("/sampleConfig3.properties").getFile()).toPath());
			MockMultipartFile sampleConfig3 = new MockMultipartFile("modelConfig", "sampleConfig3.properties", null,
					configbytes3);
			MockMultipartHttpServletRequestBuilder builder7 = MockMvcRequestBuilders.multipart("/model/configuration");
			builder7.with(new RequestPostProcessor() {
				@Override
				public MockHttpServletRequest postProcessRequest(MockHttpServletRequest request) {
					request.setMethod("PUT");
					return request;
				}
			});
			mockMvc.perform(builder7.file(sampleConfig3)).andExpect(status().isOk());

			// Use sample3.proto
			byte[] protobytes3 = Files.readAllBytes(Paths.get("src", "test", "resources", "sample3.proto"));
			MockMultipartFile proto3 = new MockMultipartFile("proto", "sample3.proto", null, protobytes3);

			MockMultipartHttpServletRequestBuilder builder8 = MockMvcRequestBuilders.multipart("/proto");
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

			byte[] resultsBinaryDefault1 = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/getBinaryDefault")
					.file(csvfile3).param("operation", "classify")).andExpect(status().isOk()).andReturn().getResponse()
					.getContentAsByteArray();

			// testing /{operation} end point, use classify in this case
			logger.info("Testing /operations/classify POST end point");
			byte[] resultsPredict1 = this.mockMvc
					.perform(post("/classify").contentType(MediaType.TEXT_PLAIN).content(resultsBinaryDefault1))
					.andExpect(status().isOk()).andReturn().getResponse().getContentAsByteArray();
			logger.info("Done testing POST /classify end point " + Arrays.toString(resultsPredict1));

			// Use sampleConfig3_2.properties which calls the classify method of the silly
			// model
			byte[] configbytes3_2 = Files.readAllBytes(
					new File(this.getClass().getResource("/sampleConfig3_2.properties").getFile()).toPath());
			MockMultipartFile sampleConfig3_2 = new MockMultipartFile("modelConfig", "sampleConfig3_2.properties", null,
					configbytes3_2);
			MockMultipartHttpServletRequestBuilder builder9 = MockMvcRequestBuilders.multipart("/model/configuration");
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
			byte[] resultsBinaryDefault2 = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/getBinaryDefault")
					.file(csvfile4).param("operation", "aggregate")).andExpect(status().isOk()).andReturn()
					.getResponse().getContentAsByteArray();

			// testing /aggregate end point
			logger.info("Testing /aggregate POST end point");
			byte[] resultsPredict2 = this.mockMvc
					.perform(post("/aggregate").contentType(MediaType.TEXT_PLAIN).content(resultsBinaryDefault2))
					.andExpect(status().isOk()).andReturn().getResponse().getContentAsByteArray();
			logger.info("Done testing POST /aggregate end point " + Arrays.toString(resultsPredict2));

			// Use sample4.proto
			byte[] protobytes4 = Files.readAllBytes(Paths.get("src", "test", "resources", "sample4.proto"));
			MockMultipartFile proto4 = new MockMultipartFile("proto", "sample4.proto", null, protobytes4);

			MockMultipartHttpServletRequestBuilder builder10 = MockMvcRequestBuilders.multipart("/proto");
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
			MockMultipartHttpServletRequestBuilder builderModelConfig4 = MockMvcRequestBuilders
					.multipart("/model/configuration");
			builderModelConfig4.with(new RequestPostProcessor() {
				@Override
				public MockHttpServletRequest postProcessRequest(MockHttpServletRequest request) {
					request.setMethod("PUT");
					return request;
				}
			});
			mockMvc.perform(builderModelConfig4.file(sampleConfig4)).andExpect(status().isOk());

			// use sample4.csv test
			byte[] csvSample4 = Files
					.readAllBytes(new File(this.getClass().getResource("/sample4.csv").getFile()).toPath());
			MockMultipartFile csvMock4 = new MockMultipartFile("csvFile", "sample4.csv", "text/csv", csvSample4);
			byte[] resultsBinaryDefault4 = this.mockMvc.perform(MockMvcRequestBuilders.multipart("/getBinaryDefault")
					.file(csvMock4).param("operation", "testEnum")).andExpect(status().isOk()).andReturn().getResponse()
					.getContentAsByteArray();

			// testing /testEnum end point
			logger.info("Testing /testEnum POST end point");
			byte[] resultsPredict4 = this.mockMvc
					.perform(post("/testEnum").contentType(MediaType.TEXT_PLAIN).content(resultsBinaryDefault4))
					.andExpect(status().isOk()).andReturn().getResponse().getContentAsByteArray();
			logger.info("Done testing POST /testEnum end point " + Arrays.toString(resultsPredict4));

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
			ProcessBuilder pb0 = new ProcessBuilder("which", "javac");
			pb0.directory(new File(simpleModelPath));

			logger.info("generateSimpleModel: executing command: \"which javac\"");
			Process p0 = pb0.start();
			// get the error stream of the process and print it
			InputStream error0 = p0.getErrorStream();
			ExecCmdUtil.printCmdError(error0);
			PrintWriter printWriter0 = new PrintWriter(p0.getOutputStream());
			BufferedReader bufferedReader0 = new BufferedReader(new InputStreamReader(p0.getInputStream()));
			ExecCmdUtil.printCmdOutput(bufferedReader0);

			printWriter0.flush();
			exitVal = p0.waitFor();

			logger.info("generateSimpleModel: Exit Value for which javac cmd: " + exitVal);

			// Find $JAVA_HOME
			// https://stackoverflow.com/questions/9368311/executing-echo-using-java-processbuilder-doesnt-interpolate-variables-output
			ImmutableList<String> cmd = ImmutableList.of("/bin/bash", "-c", "echo $JAVA_HOME");
			pb0 = new ProcessBuilder(cmd);
			logger.info("generateSimpleModel: executing command: \"echo $JAVA_HOME\"");

			p0 = pb0.start();
			// get the error stream of the process and print it
			error0 = p0.getErrorStream();
			ExecCmdUtil.printCmdError(error0);
			printWriter0 = new PrintWriter(p0.getOutputStream());
			bufferedReader0 = new BufferedReader(new InputStreamReader(p0.getInputStream()));
			ArrayList<String> output = ExecCmdUtil.printCmdOutput(bufferedReader0);
			String javaPath = "";
			if (!output.isEmpty())
				javaPath = output.get(0);
			printWriter0.flush();
			exitVal = p0.waitFor();

			logger.info("generateSimpleModel: Exit Value for which \"echo $JAVA_HOME\": " + exitVal);
			// Done finding $JAVA_HOME

			// Find jar program
			pb0 = new ProcessBuilder("which", "jar");
			pb0.directory(new File(simpleModelPath));
			logger.info("generateSimpleModel: executing command: \"which jar\"");

			p0 = pb0.start();
			// get the error stream of the process and print it
			error0 = p0.getErrorStream();
			ExecCmdUtil.printCmdError(error0);
			printWriter0 = new PrintWriter(p0.getOutputStream());
			bufferedReader0 = new BufferedReader(new InputStreamReader(p0.getInputStream()));
			ExecCmdUtil.printCmdOutput(bufferedReader0);
			printWriter0.flush();
			exitVal = p0.waitFor();

			logger.info("generateSimpleModel: Exit Value for which jar cmd: " + exitVal);
			// Done finding jar

			ProcessBuilder pb = new ProcessBuilder("javac", "SimpleMockModel.java");
			pb.directory(new File(simpleModelPath));

			logger.info("generateSimpleModel: Setting directory to : " + simpleModelPath
					+ " before building simple model class file");
			logger.info("generateSimpleModel: executing command: \"javac SimpleMockModel.java\" from directory "
					+ simpleModelPath);
			Process p = pb.start();
			// get the error stream of the process and print it
			InputStream error = p.getErrorStream();
			ExecCmdUtil.printCmdError(error);

			PrintWriter printWriter = new PrintWriter(p.getOutputStream());
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(p.getInputStream()));
			ExecCmdUtil.printCmdOutput(bufferedReader);
			bufferedReader.close();
			printWriter.flush();
			exitVal = p.waitFor();

			logger.info("generateSimpleModel: Exit Value for javac cmd: " + exitVal);
			String jarCmd = "jar";

			if (javaPath != null && javaPath.length() != 0) // use absolute path
				jarCmd = javaPath + SEP + "bin" + SEP + "jar";

			pb = new ProcessBuilder(jarCmd, "-cvf", "model.jar", "SimpleMockModel.class");

			pb.directory(new File(simpleModelPath));

			logger.info("generateSimpleModel: Setting directory to : " + simpleModelPath
					+ " before producing simple model.jar");
			logger.info("generateSimpleModel: executing command: \"" + jarCmd
					+ " -cvf model.jar SimpleMockModel.class\" from directory " + simpleModelPath);
			p = pb.start();
			// get the error stream of the process and print it
			error = p.getErrorStream();
			for (int i = 0; i < error.available(); i++) {
				logger.error("" + error.read());
			}

			printWriter = new PrintWriter(p.getOutputStream());
			bufferedReader = new BufferedReader(new InputStreamReader(p.getInputStream()));
			ExecCmdUtil.printCmdOutput(bufferedReader);

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
}
