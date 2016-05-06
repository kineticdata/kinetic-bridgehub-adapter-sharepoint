package com.kineticdata.bridgehub.adapter.sharepoint;

import com.kineticdata.bridgehub.adapter.BridgeAdapter;
import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.BridgeUtils;
import com.kineticdata.bridgehub.adapter.Count;
import com.kineticdata.bridgehub.adapter.Record;
import com.kineticdata.bridgehub.adapter.RecordList;
import com.kineticdata.commons.v1.config.ConfigurableProperty;
import com.kineticdata.commons.v1.config.ConfigurablePropertyMap;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;


public class SharepointAdapter implements BridgeAdapter {
    /*----------------------------------------------------------------------------------------------
     * PROPERTIES
     *--------------------------------------------------------------------------------------------*/
    
    /** Defines the adapter display name */
    public static final String NAME = "Sharepoint Bridge";
    
    /** Defines the logger */
    protected static final org.slf4j.Logger logger = LoggerFactory.getLogger(SharepointAdapter.class);
    
    /** Defines the collection of property names for the adapter */
    public static class Properties {
        public static final String USERNAME = "Username";
        public static final String PASSWORD = "Password";
        public static final String SERVER_URL = "Server URL";
    }
    
    private final ConfigurablePropertyMap properties = new ConfigurablePropertyMap(
        new ConfigurableProperty(SharepointAdapter.Properties.USERNAME).setIsRequired(true),
        new ConfigurableProperty(SharepointAdapter.Properties.PASSWORD).setIsRequired(true).setIsSensitive(true),
        new ConfigurableProperty(SharepointAdapter.Properties.SERVER_URL).setIsRequired(true)
    );
    
    /**
     * Structures that are valid to use in the bridge
     */
    public static final List<String> VALID_STRUCTURES = Arrays.asList(new String[] {
        "Lists"
    });
    
    private String username;
    private String password;
    private String serverUrl;
    
    /*---------------------------------------------------------------------------------------------
     * SETUP METHODS
     *-------------------------------------------------------------------------------------------*/
    
    @Override
    public void initialize() throws BridgeError {
        this.username = properties.getValue(Properties.USERNAME);
        this.password = properties.getValue(Properties.PASSWORD);
        this.serverUrl = properties.getValue(Properties.SERVER_URL);
    }

    @Override
    public String getName() {
        return NAME;
    }
    
    @Override
    public String getVersion() {
        return "1.0.0";
    }
    
    @Override
    public void setProperties(Map<String,String> parameters) {
        properties.setValues(parameters);
    }
    
    @Override
    public ConfigurablePropertyMap getProperties() {
        return properties;
    }

    @Override
    public Count count(BridgeRequest request) throws BridgeError {
        // Log the access
        logger.trace("Counting the Salesforce Records");
        logger.trace("  Structure: " + request.getStructure());
        logger.trace("  Query: " + request.getQuery());
        
        String structure = request.getStructure();
        
        if (!VALID_STRUCTURES.contains(structure)) {
            throw new BridgeError("Invalid Structure: '" + request.getStructure() + "' is not a valid structure");
        }
        
        SharepointQualificationParser parser = new SharepointQualificationParser();
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append(String.format("%s/_api/web/lists?", this.serverUrl));
        String query = parser.parse(request.getQuery(),request.getParameters());
        
        if (query != null){
            queryBuilder.append(URLEncoder.encode(query));
        }
        
        // We have to replace the encoded "&" and "=" values because the Sharepoint API
        // expects the literal values, not the encoded version.
        String url = queryBuilder.toString().replaceAll("%3D", "=").replaceAll("%26", "&");

        HttpClient client = new DefaultHttpClient();
        HttpGet get = new HttpGet(url);
        String credentials = String.format("%s:%s", this.username, this.password);
        byte[] basicAuthBytes = Base64.encodeBase64(credentials.getBytes());
        get.setHeader("Authorization", "Basic " + new String(basicAuthBytes));
        get.setHeader("Content-Type", "application/json");
        
        HttpResponse response;
        String output = "";
        
        try {
            response = client.execute(get);
            HttpEntity entity = response.getEntity();
            output = EntityUtils.toString(entity);
        } 
        catch (IOException e) {
            throw new BridgeError("Unable to make a connection to properly execute the query to Sharepoint"); 
        }

        Document doc;
        try {
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            doc = dBuilder.parse(new InputSource(new ByteArrayInputStream(output.getBytes("utf-8"))));
            
            doc.getDocumentElement().normalize();
        }
//        catch (ParserConfigurationException | SAXException | IOException e) {
        catch (Exception e) {
            logger.error("Full XML Error: " + e.getMessage());
            throw new BridgeError("Parsing of the XML response failed",e);
        }
        
        NodeList nodeList = doc.getElementsByTagName("entry");

        Long count;

        count = Long.valueOf(nodeList.getLength());
        return new Count(count);
    }

    @Override
    public Record retrieve(BridgeRequest request) throws BridgeError {
        // Log the access
        logger.trace("Retrieving ServiceNow Record");
        logger.trace("  Structure: " + request.getStructure());
        logger.trace("  Query: " + request.getQuery());
        logger.trace("  Fields: " + request.getFieldString());
        
        List<String> fields = request.getFields();
        String structure = request.getStructure();
        
        if (!VALID_STRUCTURES.contains(structure)) {
            throw new BridgeError("Invalid Structure: '" + request.getStructure() + "' is not a valid structure");
        }
        
        SharepointQualificationParser parser = new SharepointQualificationParser();
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append(String.format("%s/_api/web/lists?", this.serverUrl));
        String query = parser.parse(request.getQuery(),request.getParameters());
        
        if (query != null){
            queryBuilder.append(URLEncoder.encode(query));
        }
        
        // We have to replace the encoded "&" and "=" values because the Sharepoint API
        // expects the literal values, not the encoded version.
        String url = queryBuilder.toString().replaceAll("%3D", "=").replaceAll("%26", "&");
        
        HttpClient client = new DefaultHttpClient();
        HttpGet get = new HttpGet(url);
        String credentials = String.format("%s:%s", this.username, this.password);
        byte[] basicAuthBytes = Base64.encodeBase64(credentials.getBytes());
        get.setHeader("Authorization", "Basic " + new String(basicAuthBytes));
        get.setHeader("Content-Type", "application/json");
        
        HttpResponse response;
        String output = "";
        
        try {
            response = client.execute(get);
            HttpEntity entity = response.getEntity();
            output = EntityUtils.toString(entity);
        } 
        catch (IOException e) {
            throw new BridgeError("Unable to make a connection to properly execute the query to Sharepoint"); 
        }

        Document doc;
        try {
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            doc = dBuilder.parse(new InputSource(new ByteArrayInputStream(output.getBytes("utf-8"))));
            
            doc.getDocumentElement().normalize();
        }
//        catch (ParserConfigurationException | SAXException | IOException e) {
        catch (Exception e) {
            logger.error("Full XML Error: " + e.getMessage());
            throw new BridgeError("Parsing of the XML response failed",e);
        }
        
        NodeList nodeList = doc.getElementsByTagName("entry");
        Record record;
        
        if (nodeList.getLength() > 1) {
            throw new BridgeError("Multiple results matched an expected single match query");
        }  
        else if (nodeList.getLength() == 0) {
            record = new Record(null);
        }
        else {
            Map<String,Object> recordMap = new LinkedHashMap<String,Object>();
            if (fields == null) {
                record = new Record(null);
            } else {
                for (String field :fields) {
                    NodeList propertyList = doc.getElementsByTagName("d:" + field);
                    recordMap.put(field, propertyList.item(0).getTextContent());
                }
                record = new Record(recordMap);
            }
        }
        
        return record;
    }

    @Override
    public RecordList search(BridgeRequest request) throws BridgeError {
        // Log the access
        logger.trace("Searching ServiceNow Records");
        logger.trace("  Structure: " + request.getStructure());
        logger.trace("  Query: " + request.getQuery());
        logger.trace("  Fields: " + request.getFieldString());
        
        List<String> fields = request.getFields();
        String structure = request.getStructure();
        
        if (!VALID_STRUCTURES.contains(structure)) {
            throw new BridgeError("Invalid Structure: '" + request.getStructure() + "' is not a valid structure");
        }
        
        StringBuilder queryBuilder = new StringBuilder();
        Map<String,String> metadata = BridgeUtils.normalizePaginationMetadata(request.getMetadata());
        queryBuilder.append(String.format("%s/_api/web/lists?", this.serverUrl));
        SharepointQualificationParser parser = new SharepointQualificationParser();
        String query = parser.parse(request.getQuery(),request.getParameters());
        
        if (query != null){
            queryBuilder.append(URLEncoder.encode(query));
        }
        
        // We have to replace the encoded "&" and "=" values because the Sharepoint API
        // expects the literal values, not the encoded version.
        String url = queryBuilder.toString().replaceAll("%3D", "=").replaceAll("%26", "&");
        
        HttpClient client = new DefaultHttpClient();
        HttpGet get = new HttpGet(url);
        String credentials = String.format("%s:%s", this.username, this.password);
        byte[] basicAuthBytes = Base64.encodeBase64(credentials.getBytes());
        get.setHeader("Authorization", "Basic " + new String(basicAuthBytes));
        get.setHeader("Content-Type", "application/json");
        
        HttpResponse response;
        String output = "";
        
        try {
            response = client.execute(get);
            HttpEntity entity = response.getEntity();
            output = EntityUtils.toString(entity);
        } 
        catch (IOException e) {
            throw new BridgeError("Unable to make a connection to properly execute the"
                    + "query to Sharepoint"); 
        }

        Document doc;
        try {
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            doc = dBuilder.parse(new InputSource(new ByteArrayInputStream(output.getBytes("utf-8"))));
            
            doc.getDocumentElement().normalize();
        }
//        catch (ParserConfigurationException | SAXException | IOException e) {
        catch (Exception e) {
            logger.error("Full XML Error: " + e.getMessage());
            throw new BridgeError("Parsing of the XML response failed",e);
        }
        
        List<Record> records = new ArrayList<Record>();
        NodeList nodeList = doc.getElementsByTagName("entry");
        
        for (int i = 0; i < nodeList.getLength(); i++){
            Map<String,Object> recordMap = new LinkedHashMap<String,Object>();
            for (String field : fields) {
                NodeList propertyList = doc.getElementsByTagName("d:" + field);
                recordMap.put(field, propertyList.item(i).getTextContent());
            }
            records.add(new Record(recordMap));
        }

        // Returning the response
        return new RecordList(fields, records, metadata);
    }
    
}
