package com.nordea.payments.app;

import java.util.Map;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 
 * @author Avinash
 *
 */

@RestController
public class CustomerController {
	BatchConfiguration config = new BatchConfiguration();

	/**
	 * controller method call to display customer details
	 * 
	 * @return
	 */
	@GetMapping(path = "/list-customers")
	public Map<String, String> display() {
		Map<String, String> data = config.display();

		return data;

	}

}
