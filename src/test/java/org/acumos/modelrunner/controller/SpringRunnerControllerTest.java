package org.acumos.modelrunner.controller;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import java.io.File;
import java.nio.file.Files;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

public class SpringRunnerControllerTest extends SpringBootRunnerControllerTests {

	@Autowired
	private WebApplicationContext webApplicationContext;

	private MockMvc mockMvc;

	@Before
	public void setup() {
		mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
	}

	@Test
	public void predictTest() {

		String filePath = System.getProperty("user.dir");

		filePath = filePath + File.separator + "testdata" + File.separator + "iris5.bin";

		byte[] bFile = null;
		try {
			bFile = Files.readAllBytes(new File(filePath).toPath());

			this.mockMvc.perform(post("/predict").contentType(MediaType.TEXT_PLAIN).content(bFile));

		} catch (Exception e) {
			assert (false);
			e.printStackTrace();
		}
		assert (true);
	}
}
