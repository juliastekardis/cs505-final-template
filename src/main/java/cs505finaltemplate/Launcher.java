package cs505finaltemplate;

import cs505finaltemplate.CEP.CEPEngine;
import cs505finaltemplate.Topics.TopicConnector;
import cs505finaltemplate.Embedded.EmbeddedDBEngine;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.*;


public class Launcher {
    public static String inputStreamName;
    public static CEPEngine cepEngine;
    public static EmbeddedDBEngine embeddedEngine;
    public static TopicConnector topicConnector;
    public static final int WEB_PORT = 9000;

    public static String lastCEPOutput = "{}";
    public static HashMap<String, Integer> counts_current_cep = new HashMap<>();
    public static HashMap<String, Integer> counts_last_cep = new HashMap<>();
    public static List<String> zipList = new ArrayList<String>();
    //public static List<String> zipList = Arrays.asList("40508", "40511", "40504", "40059", "42101");
    public static Integer state_status;

    public static void main(String[] args) throws IOException {


        //startig DB/CEP init

        //READ CLASS COMMENTS BEFORE USING

        embeddedEngine = new EmbeddedDBEngine();

        cepEngine = new CEPEngine();

        System.out.println("Starting CEP...");

        inputStreamName = "testInStream";
        String inputStreamAttributesString = "zip_code string";

        String outputStreamName = "testOutStream";
        String outputStreamAttributesString = "zip_code string, count long";

        //This query must be modified.  Currently, it provides the last zip_code and total count
        //You want counts per zip_code, to say another way "grouped by" zip_code
        String queryString = " " +
                "from testInStream#window.timeBatch(15 sec) " +
                "select zip_code, count() as count " +
                "group by zip_code " +
                "insert into testOutStream; ";

        cepEngine.createCEP(inputStreamName, outputStreamName, inputStreamAttributesString, outputStreamAttributesString, queryString);

        System.out.println("CEP Started...");
        //end DB/CEP Init

        //start message collector
        Map<String,String> message_config = new HashMap<>();
        message_config.put("hostname","128.163.202.50"); //Fill config for your team in
        message_config.put("username","student");
        message_config.put("password","student01");
        message_config.put("virtualhost","16");

        topicConnector = new TopicConnector(message_config);
        topicConnector.connect();
        //end message collector

        //Embedded HTTP initialization
        startServer();

        try {
            while (true) {
                Thread.sleep(5000);
            }
        }catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void startServer() throws IOException {

        final ResourceConfig rc = new ResourceConfig()
        .packages("cs505finaltemplate.httpcontrollers");

        System.out.println("Starting Web Server...");
        URI BASE_URI = UriBuilder.fromUri("http://0.0.0.0/").port(WEB_PORT).build();
        HttpServer httpServer = GrizzlyHttpServerFactory.createHttpServer(BASE_URI, rc);

        try {
            httpServer.start();
            System.out.println("Web Server Started...");
            state_status = 0;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
