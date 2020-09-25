package asyncProxyServer;

import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.net.HttpURLConnection;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.Inet6Address;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.time.LocalTime;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.Headers;
import java.sql.*;

public class asyncProxyServer {
    public static void main(String[] args) {
        try {
            // Bind to port 8080
        	int port = 8080;
        	
        	//Obtains IP address from static method
        	String ip = findIP();
        	        	
            //Initialises HTTP Server
        	HttpServer httpServer = HttpServer.create(new InetSocketAddress(ip, port), 0);

            // Adding '/' context
            httpServer.createContext("/", new RequestHandler());
            
            //Creates ExecutorService to handle request in parallel fashion
        	ExecutorService executor = Executors.newFixedThreadPool(4);
        	
        	//Sets ExecutorService for HTTPServer
            httpServer.setExecutor(executor);
            
            // Start the server
            httpServer.start();
            System.out.println("Server started on port " + ip + ":" + port);
        } catch (IOException ex) {
            System.out.println("Error could not start server");
        }
 
    }
    
    private static class RequestHandler implements HttpHandler {
		@Override
        public void handle(HttpExchange he) throws IOException {
            System.out.println("Serving request");
            
            //Gets time of request 
            LocalTime requestTime = LocalTime.now();
            
            // Serve for POST requests only
            if (he.getRequestMethod().equalsIgnoreCase("POST")) {
 
                try {
                	String res = null;
                	
                	String response = null;
                	
                    // REQUEST Headers
                    Headers requestHeaders = he.getRequestHeaders();
                    Set<Map.Entry<String, List<String>>> entries = requestHeaders.entrySet();
                    int contentLength = Integer.parseInt(requestHeaders.getFirst("Content-length"));
                    
                    // REQUEST Body
                    InputStream is = he.getRequestBody();
                    byte[] data = new byte[contentLength];
                    int length = is.read(data);
                    
                    // RESPONSE Headers
                    Headers responseHeaders = he.getResponseHeaders();
                    
                    // Send RESPONSE Headers
                    he.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
                    
                    // RESPONSE Body
                    OutputStream os = he.getResponseBody();
                    
                    //Inserts data from request body into MySQL table
                    dbInsert(new String(data));
                    
                    //Creates thread pool to handle responses asynchronously
                    ExecutorService service = Executors.newFixedThreadPool(5);
    				
                    //Future submits getDB method
    				Future<String> future = service.submit(new GetDB());
    				
    				//Future waits 3000ms for a response
    				try {
    					res = future.get(3000L, TimeUnit.MILLISECONDS);
    				} catch (TimeoutException e) {
    					System.out.println("DATABASE RESPONSE TIMEOUT...");
    				}
    				
    				//Gets time of response
    				LocalTime responseTime = LocalTime.now();
    				
    				//Return response body to sender
    				//If database response == null returns "DATABASE_TIMEOUT" else sends table in string form
    				if (res != null) {
    					response = "RESPONSE: " + HttpURLConnection.HTTP_OK + " POST" + "\n" + 
                        		"RESPONSE_BODY: " + res + "\n" +
    							"REQUEST_TIME: " + requestTime.toString() + "\n" +
                        		"RESPONSE_TIME: " + responseTime.toString() +"\n";
                        
    				} else {
    					response = "RESPONSE: " + HttpURLConnection.HTTP_OK + " POST" + "\n" + 
                        		"RESPONSE_BODY: " + "DATABASE_TIMEOUT" + "\n" +
                        		"REQUEST_TIME: " + requestTime.toString() + "\n" +
                        		"RESPONSE_TIME: " + responseTime.toString() +"\n";

    				}
                    
    				//Writes response into OutputStream
    				os.write(response.getBytes());
    				
    				he.close();
 
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            
            // Serve GET requests
            else if (he.getRequestMethod().equalsIgnoreCase("GET")) {
            	
            	try {
            		String res = null;
            		
            		String response = null;
            		
            		System.out.println("GET request received");
            		
    				he.sendResponseHeaders(HttpURLConnection.HTTP_OK, 0);
    				
    				OutputStream os = he.getResponseBody();
                    
    				//Get from SQL database here...
    				
    				ExecutorService service = Executors.newFixedThreadPool(5);
    				
    				Future<String> future = service.submit(new GetDB());
    				
    				try {
    					res = future.get(3000L, TimeUnit.MILLISECONDS);
    				} catch (TimeoutException e) {
    					System.out.println("DATABASE RESPONSE TIMEOUT...");
    				}
    				
    				LocalTime responseTime = LocalTime.now();
    				
    				if (res != null) {
    					response = "RESPONSE: " + HttpURLConnection.HTTP_OK + " GET" + "\n" + 
                        		"RESPONSE_BODY: " + res + "\n" +
                        		"REQUEST_TIME: " + requestTime.toString() + "\n" +
                        		"RESPONSE_TIME: " + responseTime.toString() +"\n";                        
                        
    				} else {
    					response = "RESPONSE: " + HttpURLConnection.HTTP_OK + " GET" + "\n" + 
                        		"RESPONSE_BODY: " + "DATABASE_TIMEOUT" + "\n" +
                        		"REQUEST_TIME: " + requestTime.toString() + "\n" +
                        		"RESPONSE_TIME: " + responseTime.toString() +"\n";

    				}
                    
    				os.write(response.getBytes());
    				
    				he.close();
    				
    			} catch (Exception e) {
    				e.printStackTrace();
    			}	
            }
        }
    }
    
    //Class implementing Callable with return type String for Future Class
    static class GetDB implements Callable<String> {
    	@Override
    	public String call() throws Exception {
    		//IGNORE - testing code for asynchronous timeouts
    		/*double random = Math.round(Math.random());
    		Thread.sleep((long) (random * 4000));*/
    		return dbGet();
    	}
    }
    
    
    private static void dbInsert(String str) {
    	String[] strArr = str.split("&");

        Statement stmt = null;
        
        Connection conn = null;
                
        try {
           try {
              Class.forName("com.mysql.jdbc.Driver");
           } catch (Exception e) {
              System.out.println(e);
        }
        
        //Establishes connection with MySQL server on localhost:3306
        conn = (Connection) DriverManager.getConnection("jdbc:mysql://localhost:3306/db1?useSSL=false", "root", "password");
        
        System.out.println("MYSQL Connection is created successfully:");
        
        stmt = (Statement) conn.createStatement();
        
        for (String data: strArr) {
        	
        	//Parses and removes extra "'" from string
        	String[] arr = data.split("=");
        	
        	String key = arr[0].replaceAll("'", "");
        	
        	String value = arr[1].replaceAll("'", "");

        	String query;
        	
        	List<String> keyID = new ArrayList<String>();
        	
        	//Selects all columns from table
        	ResultSet rs = stmt.executeQuery("SELECT key_id FROM hashmap");
        	
        	while (rs.next()) {
        		keyID.add(rs.getString("key_id"));
        	}
        	
        	//Inserts value into table if it exists else update existing value in table
        	if (!keyID.contains(key)) {
        		query = String.format("INSERT INTO hashmap " + "VALUES ('%s', '%s')", key, value);
        		
        		stmt.executeUpdate(query);
        	} else {
        		query = String.format("UPDATE hashmap " + "SET value = '%s'" + "WHERE key_id = '%s' ", value, key);

        		stmt.executeUpdate(query);
        	}
        }
                
        System.out.println("Record is inserted in the table successfully..................");
        
        stmt.close();
        
        } catch (SQLException excep) {
        	
           excep.printStackTrace();
           
        } catch (Exception excep) {
        	
           excep.printStackTrace();
           
        } finally {
           try {
              if (stmt != null)
                 conn.close();
           } catch (SQLException se) {}
           try {
              if (conn != null)
                 conn.close();
           } catch (SQLException se) {
              se.printStackTrace();
           }  
        }
    }
    
    private static String dbGet() {
    	String result = "";
    	
        try
        {
        	// create our MySQL database connection
	        String myDriver = "org.gjt.mm.mysql.Driver";
	        Class.forName(myDriver);
	        Connection conn = (Connection) DriverManager.getConnection("jdbc:mysql://localhost:3306/db1?useSSL=false", "root", "password");
	
	          
	        // our SQL SELECT query. 
	        // if you only need a few columns, specify them by name instead of using "*"
	        String query = "SELECT * FROM hashmap";
	
	        // create the java statement
	        Statement st = conn.createStatement();
	          
	        // execute the query, and get a java resultset
	        ResultSet rs = st.executeQuery(query);
	          
	        // iterate through the java resultset
	        while (rs.next()) {
	        	result += rs.getString("key_id") + "=" + rs.getString("value") + "&";
	        }
	          
	        st.close();
	          
        }
        catch (Exception e)
        {
        	e.printStackTrace();
        }
        
        return result;
    }
        
    //Filters out IPV4 Address from network interfaces
	
    static String findIP() throws SocketException {

    	String ip = null;
    	
    	//Checks all network interfaces
    	Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
    	
 
        while (interfaces.hasMoreElements()) {
            NetworkInterface iface = interfaces.nextElement();
            // filters out 127.0.0.1 and inactive interfaces
            if (iface.isLoopback() || !iface.isUp())
                continue;

            Enumeration<InetAddress> addresses = iface.getInetAddresses();
            while(addresses.hasMoreElements()) {
                InetAddress addr = addresses.nextElement();
                //continues loop if IPV6 else assigns VAR ip to IPV4
                if (addr instanceof Inet6Address) continue;

                ip = addr.getHostAddress();
            }
        }
        //return IPV4 address
        return ip;
    }
    
}

