package rabbit_kafka_bridge;

import java.util.Date;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import Decoder.BASE64Encoder;

//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.Producer;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.zip.GZIPInputStream;

public class DisqusProducer {

	
	public static  String username;
	public static  String password;
	
	public static String connectionString;
	public static String streamURL;
	public static String outputqueue;
	public static String logFileDirectory;
	public static String logErrorFileDirectory;
	public static String adminEmail;
	public static String smtp;
	public static String smtpPort;
	public static String smtp_username;
	public static String smtp_password;
	
	
	public static String splnkIP;
	public static String splnkPort;
	
	
	public static File rfile;
	public static File efile;
	
	public static FileWriter rfop;
	public static FileWriter efop;
	
	public static int counter =0;
	
	public static Properties props = null;
	public static Producer<String, String> kproducer=null;
	
	static KafkaController kp = new KafkaController();
	
	public static Logger logger = LoggerFactory.getLogger("rabbit_kafka_bridge_logger");
	
	 public static void main(String[] args) {
		// TODO Auto-generated method stub

		/**********************************************
		
					STARTING DISQUS PRODUCER
		
		***********************************************/
		
		try {
			
			
			getConfigurations();
			errorReporting("*STARTING DISQUS STREAM COLLECTOR*");
			//rfop.write("*STARTING DISQUS STREAM COLLECTOR*"  + "  "  + DateTime.now().toString());
			//efop.write("*STARTING DISQUS STREAM COLLECTOR*"  + "  "  + DateTime.now().toString());
			
			
			
			
		} catch (MessagingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			try {
				errorReporting(e.getMessage().toString() + " " + " MAIN FUNCTION.");
			} catch (MessagingException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
		 
		}
		
		
		
		props = new Properties();
	    //props.put("metadata.broker.list", "192.168.7.124:9092,192.168.7.125:9092,192.168.7.126:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
	    props.put("broker.list","192.168.7.124:9092,192.168.7.125:9092,192.168.7.126:9092");
	    props.put("request.required.acks", "1");
	    //props.put("topic.metadata.refresh.interval.ms", "1");
	    props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
	    //props.put("enable.auto.cimmit","true");
	    //props.put("auto.commit.interval.ms","10");
	        
	   /* org.apache.kafka....*/     
	    //props.put("bootstrap.servers","192.168.7.124:9092,192.168.7.125:9092,192.168.7.126:9092");
	   //props.put("key.serializer", StringSerializer.class.getName());
	    //props.put("value.serializer", StringSerializer.class.getName());

	    
	    
		
	    ProducerConfig config = new ProducerConfig(props);
	    kproducer = new Producer<String, String>(config);
 
	    
	    
		 try {
			  
			 
			initializeAppLogs(); 
			createStreamConnection();
			
				
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				try {
					
					errorReporting(e1.getMessage().toString());
					
				} catch (MessagingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			  
		
	}
	
	 private static void getConfigurations() {
			
		 Properties fileProp = new Properties();
		 try {
				
				
				
		    fileProp.load(DisqusProducer.class.getClassLoader().getResourceAsStream("file.writer.properties"));
		    System.out.println("READING CONFIG PROPERTIES FILE...");
	   		
		 
		    connectionString = fileProp.getProperty("ConnectionString");
			logFileDirectory = fileProp.getProperty("logFileDirectory");
			logErrorFileDirectory = fileProp.getProperty("logErrorFileDirectory");
			adminEmail = fileProp.getProperty("adminEmail");
			smtp = fileProp.getProperty("smtp");
			smtpPort = fileProp.getProperty("smtpPort");
			smtp_username = fileProp.getProperty("smtp_username");
			smtp_password = fileProp.getProperty("smtp_password");
			splnkIP = fileProp.getProperty("splnkIP");
			splnkPort = fileProp.getProperty("splnkPort");
			username = fileProp.getProperty("username");
			password = fileProp.getProperty("password");
			streamURL =fileProp.getProperty("streamUrl");
			
			
			
			rfop = new FileWriter(logFileDirectory);
			efop = new FileWriter(logErrorFileDirectory);
		} catch (IOException io) {
			io.printStackTrace();
		} 		
			
		}
	 
	 private static void createStreamConnection(){
		 
		 HttpURLConnection connection = null;
	     InputStream inputStream = null;

	        try {
	        	
	        	System.out.println("CREATE STREAM CONNECTION...");
	        	rfop.write("CREATE STREAM CONNECTION..."  + "  "  + DateTime.now().toString());
				
	        	//streamURL="https://gnip-api.twitter.com/rules/powertrack/accounts/Sc2corp/publishers/twitter/prod.json";
	        	//username="guidryr@sc2corp.com";
	        	//password="Century.diaL!fountAin";
	        	
	        	//streamURL = "https://streams.services.disqus.com/socialgist_new";
	        	
	            connection = getConnection(streamURL, username, password);

	            inputStream = connection.getInputStream();
	            int responseCode = connection.getResponseCode();

	            if (responseCode >= 200 && responseCode <= 299) {

	            	System.out.println("...STREAM CONNECTION SUCCESSFUL!");
		 	   	    
	            	rfop.write("...STREAM CONNECTION SUCCESSFUL!"  + "  "  + DateTime.now().toString());
		        	
	            	
	               	BufferedReader reader = new BufferedReader(new InputStreamReader(new StreamingGZIPInputStream(inputStream), "UTF-8"));
			        String line = reader.readLine();

	                while(line != null){
	                    
	                    line = reader.readLine();
	                    if(line.isEmpty()){
	                    	continue;
	                    }
	                    counter+=1;
	                    
	                    kp.sendMessage(line);
	                    System.out.println("SENT MESSAGE : --- CONFIRMED." + counter);	                    
	                    
	                }
	            } else {
	               
	            	handleNonSuccessResponse(connection);
	            }
	        } catch (Exception e) {
	            e.printStackTrace();
	            if(e.getMessage().toString().contains("reset")){
	            	connection.disconnect();
	            	createStreamConnection();
	            	
	            }
	            try {
					efop.write(e.getMessage().toString());
				} catch (IOException e4) {
					// TODO Auto-generated catch block
					e4.printStackTrace();
				}
			            try {
							
							errorReporting(e.getMessage().toString());
							efop.write(e.getMessage().toString());
							
						} catch (MessagingException e3) {
							// TODO Auto-generated catch block
							e3.printStackTrace();
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
			            
	            
	            if (connection != null) {
	                try {
						
	                 	handleNonSuccessResponse(connection);
	                	
	                	
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
						
						try {
							
							errorReporting(e1.getMessage().toString());
							
						} catch (MessagingException e2) {
							// TODO Auto-generated catch block
							e2.printStackTrace();
						}
						
						
					}
	            }
	        } finally {
	            if (inputStream != null) {
	                try {
	                	
						inputStream.close();
						
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	            }
	        }
		 
		 
		 
	 }
	 
	 private static HttpURLConnection getConnection(String urlString, String username, String password) throws IOException {
	        URL url = new URL(urlString);

	        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
	        connection.setReadTimeout(1000 * 60 * 60);
	        connection.setConnectTimeout(1000 * 10);

	        connection.setRequestProperty("Authorization", createAuthHeader(username, password));
	        connection.setRequestProperty("Accept-Encoding", "gzip");

	   return connection;
	 }
	 
	 private static String createAuthHeader(String username, String password) throws UnsupportedEncodingException {
	        BASE64Encoder encoder = new BASE64Encoder();
	        String authToken = username + ":" + password;
	        return "Basic " + encoder.encode(authToken.getBytes());
	  }
	 	 
	 private static void handleNonSuccessResponse(HttpURLConnection connection) throws IOException {
	        int responseCode = connection.getResponseCode();
	        String responseMessage = connection.getResponseMessage();
	        System.out.println("Non-success response: " + responseCode + " -- " + responseMessage);
	 }
	 	 
	 public static void initializeAppLogs() throws MessagingException{
		  
		  String logPath = logFileDirectory;
		  String logErrPath = logErrorFileDirectory;
		  
		  SimpleDateFormat ft = new SimpleDateFormat("yyyyMMddhhmmssa");
		  Date dNow = new Date();
		  
		  rfile = new File(logPath + "\\DISQUS_PRODUCER_" + ft.format(dNow) + ".txt");
		  efile = new File(logErrPath + "\\DISQUS_PRODUCER_ERR_" + ft.format(dNow) + ".txt");
		  
		  try {
			  
			rfop = new FileWriter(rfile);
			efop = new FileWriter(efile);
			
			rfile.createNewFile();
			efile.createNewFile();
	       
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			errorReporting(e.getMessage().toString());
		}
		  
	 }
	 
	 private static void errorReporting(String errMessage) throws MessagingException{
			// email client handling error messages
			String to_email="zfareed@boardreader.com";
			String from_email="zfareed@boardreader.com";
			String host_server="192.168.5.7";
			
			Properties properties = System.getProperties();
			properties.setProperty("mail.smtp.host",host_server);
			properties.setProperty("mail.smtp.user","rainmaker");
			properties.setProperty("mail.smtp.password", "97CupChamps");
			properties.setProperty("mail.smtp.port","25");
			
			Session session= Session.getDefaultInstance(properties);
			
			MimeMessage message = new MimeMessage(session);
			message.setFrom(new InternetAddress(from_email));
			message.addRecipient(Message.RecipientType.TO, new InternetAddress(to_email));
			message.setSubject("DISQUS_PRODUCER");
			message.setContent(errMessage,"text/html");
			//Transport.send(message);
			System.out.println("MESSAGE SENT - WOO WOO!");
			
		}
	 
}
