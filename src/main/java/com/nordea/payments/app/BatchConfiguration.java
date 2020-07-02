package com.nordea.payments.app;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.oxm.xstream.XStreamMarshaller;
import org.springframework.stereotype.Service;

import com.nordea.payments.data.Customer;
import com.nordea.payments.data.CustomerPOJO;



/**
 * @author Avinash
 * Configuration class to read and write data from xml to database and vice versa.
 * Includes 
 */

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
	
	private static final Logger log = LoggerFactory.getLogger(BatchConfiguration.class);


	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	
	public static Map<String,String> data = new HashMap<String, String>();

	
	/**
	 * This method reads input file from classpath
	 * @return StaxEventItemReader 
	 */
	
	public StaxEventItemReader<Customer> fileReader(){
		StaxEventItemReader<Customer> reader = new StaxEventItemReader<Customer>();
		reader.setResource(new ClassPathResource("customer.xml"));
		reader.setFragmentRootElementName("customer");
		
		Map<String,Class> aliases = new HashMap<>();
		aliases.put("customer", Customer.class);
		
		XStreamMarshaller marshaller = new XStreamMarshaller();
		marshaller.setAliases(aliases);
		
		reader.setUnmarshaller(marshaller);
		
		return reader;
	}

	
	/**
	 * Method to write data to hsqldb
	 * @param datasource
	 * @return JdbcBatchItemWriter
	 */
	
	@Bean
	public JdbcBatchItemWriter<Customer> writer(DataSource datasource){
		log.info("METHOD CALLED DATA WRITER");
		return new JdbcBatchItemWriterBuilder<Customer>()
				.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
				.sql("INSERT INTO customer (id, name) VALUES (?,?)")
				.dataSource(datasource)
				.itemPreparedStatementSetter(new UserItemPreparedStatement())
				.build();
	    
	}
	
	
	/**
	 * Innner class to set values to placeholders of query statement
	 * @author Avinash
	 * 
	 */
	private class UserItemPreparedStatement implements ItemPreparedStatementSetter<Customer>{

		@Override
		public void setValues(Customer item, PreparedStatement ps) throws SQLException {
			ps.setString(1, item.getId());
			ps.setString(2, item.getName());
			
		}
		
	}
	
	
	/**
	 * Method to read data from hsqldb
	 * @param datasource
	 * @return
	 */
	
	 @Bean(destroyMethod = "")
	 public JdbcCursorItemReader<CustomerPOJO> dataReader(DataSource datasource){
	  JdbcCursorItemReader<CustomerPOJO> reader = new JdbcCursorItemReader<CustomerPOJO>();
	  reader.setDataSource(datasource);
	  reader.setSql("SELECT id,name FROM customer");
	  reader.setRowMapper(new UserRowMapper());
	  
	  return reader;
	 }
	 
	 
	 public class UserRowMapper implements RowMapper<CustomerPOJO>{

	  @Override
	  public CustomerPOJO mapRow(ResultSet rs, int rowNum) throws SQLException {
		  CustomerPOJO customer = new CustomerPOJO();
		  customer.setId(rs.getString("id"));
		  customer.setName(rs.getString("name"));
		  log.info("CUSTOMER :"+customer.getId());
		  data.put(customer.getId(), customer.getName());
		  display();
	   return customer;
	  }
	  
	 }
	 
	 
	 /**
	  * Method to write data to csv 
	  * @return
	  */
	 
	 @Bean
	 public FlatFileItemWriter<CustomerPOJO> dataWriter(){
		 
	  FlatFileItemWriter<CustomerPOJO> writer = new FlatFileItemWriter<>();
	  writer.setResource(new ClassPathResource("users.csv"));
	  writer.setAppendAllowed(true);
	  writer.setLineAggregator(new DelimitedLineAggregator<CustomerPOJO>() {{
	   setDelimiter(",");
	   setFieldExtractor(new BeanWrapperFieldExtractor<CustomerPOJO>() {{
	    setNames(new String[] { "id", "name" });
	   }});
	  }});
	  
	  return writer;
	 }
	 
	 
	 /**
	  * Step to start reading data from xml and write data to hsqldb
	  * @param writer
	  * @return
	  */
	 @Bean
	public Step step1(JdbcBatchItemWriter<Customer> writer) {	
			return stepBuilderFactory.get("step1")
					.<Customer, Customer> chunk(10)
					.reader(fileReader())
					.writer(writer)
					//.taskExecutor(taskExecutor())
					.build();
	}
	 
	 
	 /**
	  * Step to start reading data from hsql db
	  * @param reader
	  * @return
	  */
	 @Bean
	 public Step step2(JdbcCursorItemReader<CustomerPOJO> reader) {
	  return stepBuilderFactory.get("step2").<CustomerPOJO, CustomerPOJO> chunk(10)
	    .reader(reader)
	    .writer(dataWriter())
	    .build();
	 }
	
	
	
	@Bean
	public TaskExecutor taskExecutor(){
	    SimpleAsyncTaskExecutor asyncTaskExecutor=new SimpleAsyncTaskExecutor("spring_batch");
	    asyncTaskExecutor.setConcurrencyLimit(5);
	    return asyncTaskExecutor;
	}
	
	
	
	@Bean
	public Job importUserJob(JobCompletionNotificationListener listener, JdbcBatchItemWriter<Customer> writer, JdbcCursorItemReader<CustomerPOJO> reader) {
		
		return jobBuilderFactory.get("importUserJob")
				.incrementer(new RunIdIncrementer())
				.listener(listener)
				.start(step1(writer))
				.next(step2(reader))
				.build();
		
	}	
	
	
	
	/**
	 * Method to display data retrieved from database
	 * @return
	 */
	@Cacheable(value="customer")
	public Map<String,String> display() {
		log.info("CALLING DISPLAY");
		data.forEach((key, value) -> {
			log.info("key: " + key);
			log.info(", Value: " + value);
		});
		return data;
	}

}
