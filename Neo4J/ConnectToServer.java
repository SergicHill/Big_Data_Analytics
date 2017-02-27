//package edu.hu.neo4j.newproject;
package org.neo4j.graphproject;

import java.net.URI;
import java.net.URISyntaxException;

import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class ConnectToServer
{
    private static final String SERVER_ROOT_URI = "http://localhost:7474/db/data/";

    public static void main( String[] args ) throws URISyntaxException
    {
          checkDatabaseIsRunning();

          sendTransactionalCypherQuery( "CREATE (m:Movi { name: 'John Wick' });" );
          
          sendTransactionalCypherQuery( "MATCH  (m: Movi { name: 'John Wick' }) CREATE (:Actor { name: 'Keanu Reeves'})  - [:ACTS_IN ]->(m); " );
          sendTransactionalCypherQuery( "MATCH  (m: Movi { name: 'John Wick' })  CREATE (:Actor { name: 'William Dafoe '}) - [:ACTS_IN ]->(m); " );
          sendTransactionalCypherQuery( "MATCH  (m: Movi { name: 'John Wick' })  CREATE (:Actor { name: 'Michael Nyquist '}) - [:ACTS_IN ]->(m);" );
          sendTransactionalCypherQuery( "MATCH  (m: Movi { name: 'John Wick' })  CREATE (:Director { name: 'Chad Stahelski '}) - [:DIRECT_IN ]->(m);" );
          sendTransactionalCypherQuery( "MATCH  (m: Movi{ name: 'John Wick' })  CREATE (:Director { name: 'David Leitch '})  - [:DIRECT_IN ]->(m);" );

          
          
          
           sendTransactionalCypherQuery( "MATCH (n)  RETURN n" );
    }
    

    
    private static void sendTransactionalCypherQuery(String queryPart1, String queryPart2, String props_key, String props_value) {
        // START SNIPPET: queryAllNodes
        final String txUri = SERVER_ROOT_URI + "transaction/commit";
        WebResource resource = Client.create().resource( txUri );

String payload = "{\"statements\" : [ {\"statement\" : \" "  + queryPart1 + "{props}"  + queryPart2 +  "\" ,\"parameters\":{ \"props\":{ \"" +props_key +"\" : \""+ props_value+"\"}}} ]}";



		
        ClientResponse response = resource
                .accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON )
                .entity( payload )
                .post( ClientResponse.class );
        
        System.out.println( String.format(
                "POST [%s] to [%s], status code [%d], returned data: "
                        + System.lineSeparator() + "%s",
                payload, txUri, response.getStatus(),
                response.getEntity( String.class ) ) );
        
        response.close();
        // END SNIPPET: queryAllNodes
    }
    

    private static void sendTransactionalCypherQuery(String query) {
        // START SNIPPET: queryAllNodes
        final String txUri = SERVER_ROOT_URI + "transaction/commit";
        WebResource resource = Client.create().resource( txUri );

        String payload = "{\"statements\" : [ {\"statement\" : \"" +query + "\"} ]}";
        ClientResponse response = resource
                .accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON )
                .entity( payload )
                .post( ClientResponse.class );
        
        System.out.println( String.format(
                "POST [%s] to [%s], status code [%d], returned data: "
                        + System.lineSeparator() + "%s",
                payload, txUri, response.getStatus(),
                response.getEntity( String.class ) ) );
        
        response.close();
        // END SNIPPET: queryAllNodes
    }

    @SuppressWarnings("unused")
	private static void findSingersInBands( URI startNode )
            throws URISyntaxException
    {
        // START SNIPPET: traversalDesc
        // TraversalDefinition turns into JSON to send to the Server
        TraversalDefinition t = new TraversalDefinition();
        t.setOrder( TraversalDefinition.DEPTH_FIRST );
        t.setUniqueness( TraversalDefinition.NODE );
        t.setMaxDepth( 10 );
        t.setReturnFilter( TraversalDefinition.ALL );
        t.setRelationships( new Relation( "singer", Relation.OUT ) );
        // END SNIPPET: traversalDesc

        // START SNIPPET: traverse
        URI traverserUri = new URI( startNode.toString() + "/traverse/node" );
        WebResource resource = Client.create()
                .resource( traverserUri );
        String jsonTraverserPayload = t.toJson();
        ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON )
                .entity( jsonTraverserPayload )
                .post( ClientResponse.class );

        System.out.println( String.format(
                "POST [%s] to [%s], status code [%d], returned data: "
                        + System.lineSeparator() + "%s",
                jsonTraverserPayload, traverserUri, response.getStatus(),
                response.getEntity( String.class ) ) );
        response.close();
        // END SNIPPET: traverse
    }
    
    // START SNIPPET: insideAddMetaToProp
    @SuppressWarnings("unused")
	private static void addMetadataToProperty( URI relationshipUri,
            String name, String value ) throws URISyntaxException
    {
        URI propertyUri = new URI( relationshipUri.toString() + "/properties" );
        String entity = toJsonNameValuePairCollection( name, value );
        WebResource resource = Client.create()
                .resource( propertyUri );
        ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON )
                .entity( entity )
                .put( ClientResponse.class );

        System.out.println( String.format(
                "PUT [%s] to [%s], status code [%d]", entity, propertyUri,
                response.getStatus() ) );
        response.close();
    }

    // END SNIPPET: insideAddMetaToProp

    private static String toJsonNameValuePairCollection( String name,
            String value )
    {
        return String.format( "{ \"%s\" : \"%s\" }", name, value );
    }

    private static URI createNode()
    {
        // START SNIPPET: createNode
    	// final String txUri = SERVER_ROOT_URI + "transaction/commit";
        final String nodeEntryPointUri = SERVER_ROOT_URI + "node";
        // http://localhost:7474/db/data/node

        WebResource resource = Client.create()
                .resource( nodeEntryPointUri );
        // POST {} to the node entry point URI
        ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON )
                .entity( "{}" )
                .post( ClientResponse.class );

         final URI location = response.getLocation();
        System.out.println( String.format(
                "POST to [%s], status code [%d], location header [%s]",
                nodeEntryPointUri, response.getStatus(), location.toString()));
        response.close();

        return location;
        // END SNIPPET: createNode
    }

    // START SNIPPET: insideAddRel
    @SuppressWarnings("unused")
	private static URI addRelationship( URI startNode, URI endNode,
            String relationshipType, String jsonAttributes )
            throws URISyntaxException
    {
        URI fromUri = new URI( startNode.toString() + "/relationships" );
        String relationshipJson = generateJsonRelationship( endNode,
                relationshipType, jsonAttributes );

        WebResource resource = Client.create()
                .resource( fromUri );
        // POST JSON to the relationships URI
        ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON )
                .entity( relationshipJson )
                .post( ClientResponse.class );

        final URI location = response.getLocation();
        System.out.println( String.format(
                "POST to [%s], status code [%d], location header [%s]",
                fromUri, response.getStatus(), location.toString() ) );

        response.close();
        return location;
    }
    // END SNIPPET: insideAddRel

    private static String generateJsonRelationship( URI endNode,
            String relationshipType, String... jsonAttributes )
    {
        StringBuilder sb = new StringBuilder();
        sb.append( "{ \"to\" : \"" );
        sb.append( endNode.toString() );
        sb.append( "\", " );

        sb.append( "\"type\" : \"" );
        sb.append( relationshipType );
        if ( jsonAttributes == null || jsonAttributes.length < 1 )
        {
            sb.append( "\"" );
        }
        else
        {
            sb.append( "\", \"data\" : " );
            for ( int i = 0; i < jsonAttributes.length; i++ )
            {
                sb.append( jsonAttributes[i] );
                if ( i < jsonAttributes.length - 1 )
                { // Miss off the final comma
                    sb.append( ", " );
                }
            }
        }

        sb.append( " }" );
        return sb.toString();
    }

 
    
    private static void addProperty( URI nodeUri, String propertyName,
            String propertyValue )
    {
        // START SNIPPET: addProp
        String propertyUri = nodeUri.toString() + "/properties/" + propertyName;
        // http://localhost:7474/db/data/node/{node_id}/properties/{property_name}

        WebResource resource = Client.create()
                .resource( propertyUri );
        ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
                .type( MediaType.APPLICATION_JSON )
                .entity( "\"" + propertyValue + "\"" )
                .put( ClientResponse.class );

        System.out.println( String.format( "PUT to [%s], status code [%d]",
                propertyUri, response.getStatus() ) );
        response.close();
        // END SNIPPET: addProp
    }

    private static void checkDatabaseIsRunning()
    {
        // START SNIPPET: checkServer
        WebResource resource = Client.create()
                .resource( SERVER_ROOT_URI );
        ClientResponse response = resource.get( ClientResponse.class );

        System.out.println( String.format( "GET on [%s], status code [%d]",
                SERVER_ROOT_URI, response.getStatus() ) );
        response.close();
        // END SNIPPET: checkServer
    }
}