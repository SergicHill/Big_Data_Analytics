private static void sendTransactionalCypherQuery(String query, String props_key, String props_value) {
        // START SNIPPET: queryAllNodes
        final String txUri = SERVER_ROOT_URI + "transaction/commit";
        WebResource resource = Client.create().resource( txUri );
String payload = "{\"statements\" : [ {\"statement\" : \"" +query + " \"} ]}";

String payload = "{\"statements\" : [ {\"statement\" : \" "                             +query       +  " ,\"parameters\":{ \"props\":{ \"" +props_key +"\" : \""+ props_value+"\"}}} ]}";

/**		
"statements"     : [{ \"statement\":   " CREATE (matrix1:Movie{props} ) RETURN matrix1" ,  "parameters": { "props" :{ "title"              : "The Matrix", "year" : "1999-03-31"  
}
}
}
*/

		
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
