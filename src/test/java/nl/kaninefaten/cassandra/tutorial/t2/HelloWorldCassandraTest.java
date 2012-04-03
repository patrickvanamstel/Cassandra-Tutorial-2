package nl.kaninefaten.cassandra.tutorial.t2;

import static org.junit.Assert.fail;

import java.util.List;

import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

/**
 * HelloWorld Unit test for an embedded cassandra server.
 * <p>
 * <ul>
 * 	<li>This class creates an embedded server starts it.
 * 	<li>Creates a keyspace.
 * 	<li>Deletes a keyspace.
 *  <li>Stops de server and exits the jvm
 * </ul>
 * 
 * 
 * @author Patrick van Amstel
 * @date 2012 04 02
 *
 */
public class HelloWorldCassandraTest  {

	/** Name of test cluster*/
	public static String clusterName = "TestCluster";
	
	/** Name and port of test cluster*/
	public static String host = "localhost:9171";
	
	/** Name of key space to create*/
	public static String keyspaceName = "keySpaceName";

	// Unit rule that starts server on startup and closes the server when shutdown
	@Rule
	public ExternalResource resource = new ExternalResource() {

		@Override
		protected void before() throws Throwable {
			EmbeddedCassandraServerHelper.startEmbeddedCassandra();
		};

		@Override
		protected void after() {
			EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
		};
	};
	
	@Test
	public void test() {

		boolean findKeySpace = false;
		try {
			// Gets the thrift client with name an host
			Cluster cluster = HFactory.getOrCreateCluster(clusterName,host);
			
			// Creates the key space update , without any column defenitions
			KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(keyspaceName,                 
                    ThriftKsDef.DEF_STRATEGY_CLASS,  
                    1, 
                    null);
			
			// Executes the update
			cluster.addKeyspace(newKeyspace);
			
			// Fetch all keyspaces
			List <KeyspaceDefinition> keys = cluster.describeKeyspaces();
			
			// Does the fetched list contain the created keyspace
			for (KeyspaceDefinition keyspaceDefinition : keys){
				String keyName = keyspaceDefinition.getName();
				if (keyName.equals(keyspaceName)){
					findKeySpace = true;
				}
			}
		} catch (Throwable t) {
			fail(t.toString());
		}
		
		if (!findKeySpace){
			fail("Could not find keySpace " + keyspaceName);
		}
	}


}
