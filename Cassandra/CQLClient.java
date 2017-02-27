/**
 * Illustrates connection and simple queries in Cassandra using Java
 *  @author Serguey Khovansky
 *  @version: March 2016
 * Write a simple Java client starting from the attached Java class CQLClient 
 * to your Java project. As you can see this class performs basic CQL operations on your Cassandara database. 
 * It opens a session to Cassandra cluster, creates a keyspace, creates new table, inserts and queries some
 *  rows in that table.
 *  Use eclipse
 * Download Binary driver  cassandra-java-driver-2.1.6.tar.gz
 * Untar the file using command
 * $ tar zxvf cassandra-java-driver-2.1.6.tar
 * Place on the Path all .jar files in all folders.
 *
 */
	
	import com.datastax.driver.core.Cluster;
	import com.datastax.driver.core.Host;
	import com.datastax.driver.core.Metadata;
	import com.datastax.driver.core.Session;
	import com.datastax.driver.core.ResultSet;
	import com.datastax.driver.core.Row;
	
	
	/**
	* This class make connection to db Cassandra, makes a table and sets simple queries
	*/
	public class CQLClient {
	   private Cluster cluster;
	   private Session session;
	   	 
		 /** 
		  * connection to db
		  * @param node  location of the db
	     */
	   public void connect(String node) {
	      cluster = Cluster.builder()
	            .addContactPoint(node).build();
		// mykeyspace2 is the namespace where all the relevant tables are located, its name is the user's choice		
	      session = cluster.connect("mykeyspace2");
	      Metadata metadata = cluster.getMetadata();
	      System.out.printf("Connected to cluster: %s\n", 
	            metadata.getClusterName());
	      for ( Host host : metadata.getAllHosts() ) {
	         System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
	               host.getDatacenter(), host.getAddress(), host.getRack());
	      }
	   }

	   	/**
		* Creates the keyspace and tables
		*/
	   public void createSchema() {
		   try{
		   session.execute("CREATE KEYSPACE mykeyspace2 WITH replication " + 
				      "= {'class':'SimpleStrategy', 'replication_factor':1};");
		   } catch(Exception e){};
		   
		   try{
		   session.execute(
				      "CREATE TABLE mykeyspace2.Person (" +
					            "first_name varchar PRIMARY KEY," + 
					            "last_name varchar," + 
					            "city varchar," + 
					            "phone1 int," + 
					            "phone2 int," + 
					            "phone3 int" + 
					            ");"
				   );
		   } catch( Exception e ){};
		   
	   }
	   public void loadData() {
		   
		   session.execute(
				      "INSERT INTO mykeyspace2.Person (first_name, "
				      + "last_name, "
				      + "city,"
				      + "phone1, "
				      + "phone2,"
				      + "phone3) " +
				      "VALUES (" +
				          "'John2'," +
				          "'Smith2'," +
				          "'Town'," +
				          "23445," +
				          "23421," +
				          "45567);");
		
		   
		   session.execute(
				      "INSERT INTO mykeyspace2.Person (first_name, last_name, city,phone1, phone2, phone3) " +
				      "VALUES (" +
				          "'John3'," +
				          "'Smith3'," +
				          "'Town3'," +
				          "23446," +
				          "23426," +
				          "45578);");
		   
		   session.execute(
				      "INSERT INTO mykeyspace2.Person (first_name, last_name, city,phone1, phone2, phone3) " +
				      "VALUES (" +
				          "'John4'," +
				          "'Smith4'," +
				          "'Town4'," +
				          "23447," +
				          "23427," +
				          "45579);");  
	   }
	   
	   /**
		* Query the tables
		*/	   
	   public void querySchema(){
		   ResultSet results = session.execute("SELECT * FROM mykeyspace2.Person ");
		   System.out.println(String.format("%-12s\t%-12s\t%-10s\t%-10s\t%-10s\t%-10s\n%s", "First_name", "Last_name", "City","Phone1","Phone2","Phone3",
			    	  "-----------+---------------+--------------+---------------+-----------------+---------------+"));
		   for (Row row : results) {
			   
			    System.out.println(String.format("%-12s\t%-12s\t%-10s\t%-10s\t%-10s\t%-10s",
			    		row.getString("first_name"),row.getString("last_name"),row.getString("city"),
			    row.getInt("phone1"),  row.getInt("phone2"), row.getInt("phone3") ));
			}
			System.out.println();
	   }
	   
	   
	   	/**
		* Close the tables
		*/	    
	   public void close() {
	      cluster.close(); // .shutdown();
	   }

	   public static void main(String[] args) {
	      CQLClient client = new CQLClient();
	      client.connect("127.0.0.1");
	      client.createSchema();
          client.loadData();
	      client.querySchema();
	      client.close();
	   }
	}
