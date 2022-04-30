package cs505finaltemplate.httpcontrollers;

import com.google.gson.Gson;
import cs505finaltemplate.Launcher;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PathParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.*;

@Path("/api")
public class API {

    @Inject
    private javax.inject.Provider<org.glassfish.grizzly.http.server.Request> request;

    private Gson gson;

    public API() {
        gson = new Gson();
    }


    @GET
    @Path("/getteam")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getteam() {
        String responseString = "{}";
        try {
            Map<String,String> responseMap = new HashMap<>();
            responseMap.put("team_name", "Julia's Team");
            responseMap.put("Team_members_sids", "[12229023]");
            responseMap.put("app_status_code","1");

            responseString = gson.toJson(responseMap);


        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getlastcep")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAccessCount(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        try {

            //generate a response
            Map<String,String> responseMap = new HashMap<>();
            responseMap.put("lastoutput",Launcher.lastCEPOutput);
            responseString = gson.toJson(responseMap);

        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/zipalertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getZipList(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        try {

            //generate a response
            Map<String,List> responseMap = new HashMap<>();
            responseMap.put("zipList",Launcher.zipList);
            responseString = gson.toJson(responseMap);

        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/alertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAlertList(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        try {

            //generate a response
            Map<String,Integer> responseMap = new HashMap<>();
            responseMap.put("state_status",Launcher.state_status);
            responseString = gson.toJson(responseMap);

        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getpatientstatus/{hospital_id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPatientStatusByHospital(@PathParam("hospital_id") Integer hospital_id) {
        String inPatientQueryString = null;
        String ICUQueryString = null;
        String ventilatorQueryString = null;
        String responseString = "{}";
        //fill in the query

        // in-patient query
        inPatientQueryString = "SELECT COUNT(*) AS in_patient_count, " +
        "   CAST(CAST(CASE WHEN COUNT(*) > 0  THEN SUM(CASE WHEN B.vaccination_id is not null then 1 ELSE 0 END) ELSE 0 END AS DECIMAL) / CAST(CASE WHEN COUNT(*) > 0 THEN COUNT(*) ELSE 1 END AS DECIMAL) AS DECIMAL(3,2)) AS in_patient_vax" +
        "   FROM hospital_data A" +
        "   LEFT JOIN vax_data B" +
        "   ON A.patient_mrn = B.patient_mrn" +
        "   WHERE A.hospital_id = " + hospital_id + 
        "   AND A.patient_status = 1";

        // ICU query
        ICUQueryString = "SELECT COUNT(*) AS icu_patient_count, " +
        "   CAST(CAST(CASE WHEN COUNT(*) > 0  THEN SUM(CASE WHEN B.vaccination_id is not null then 1 ELSE 0 END) ELSE 0 END AS DECIMAL) / CAST(CASE WHEN COUNT(*) > 0 THEN COUNT(*) ELSE 1 END AS DECIMAL) AS DECIMAL(3,2)) AS icu_patient_vax" +
        "   FROM hospital_data A" +
        "   LEFT JOIN vax_data B" +
        "   ON A.patient_mrn = B.patient_mrn" +
        "   WHERE A.hospital_id = " + hospital_id + 
        "   AND A.patient_status = 2";

        // Ventilator query
        ventilatorQueryString = "SELECT COUNT(*) AS patient_vent_count, " +
        "   CAST(CAST(CASE WHEN COUNT(*) > 0  THEN SUM(CASE WHEN B.vaccination_id is not null then 1 ELSE 0 END) ELSE 0 END AS DECIMAL) / CAST(CASE WHEN COUNT(*) > 0 THEN COUNT(*) ELSE 1 END AS DECIMAL) AS DECIMAL(3,2)) AS patient_vent_vax" +
        "   FROM hospital_data A" +
        "   LEFT JOIN vax_data B" +
        "   ON A.patient_mrn = B.patient_mrn" +
        "   WHERE A.hospital_id = " + hospital_id + 
        "   AND A.patient_status = 3";

        List<Map<String,String>> accessMapList = Launcher.embeddedEngine.getPatientData(inPatientQueryString, ICUQueryString, ventilatorQueryString);
        responseString = gson.toJson(accessMapList);
        responseString = responseString.replace("},{", ",");
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getpatientstatus")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPatientStatus(@HeaderParam("X-Auth-API-Key") String authKey) {
        
        String inPatientQueryString = null;
        String ICUQueryString = null;
        String ventilatorQueryString = null;
        String responseString = "{}";
        //fill in the query

        // in-patient query
        inPatientQueryString = "SELECT COUNT(*) AS in_patient_count, " +
        "   CAST(CAST(CASE WHEN COUNT(*) > 0  THEN SUM(CASE WHEN B.vaccination_id is not null then 1 ELSE 0 END) ELSE 0 END AS DECIMAL) / CAST(CASE WHEN COUNT(*) > 0 THEN COUNT(*) ELSE 1 END AS DECIMAL) AS DECIMAL(3,2)) AS in_patient_vax" +
        "   FROM hospital_data A" +
        "   LEFT JOIN vax_data B" +
        "   ON A.patient_mrn = B.patient_mrn" +
        "   WHERE A.patient_status = 1";

        // ICU query
        ICUQueryString = "SELECT COUNT(*) AS icu_patient_count, " +
        "   CAST(CAST(CASE WHEN COUNT(*) > 0  THEN SUM(CASE WHEN B.vaccination_id is not null then 1 ELSE 0 END) ELSE 0 END AS DECIMAL) / CAST(CASE WHEN COUNT(*) > 0 THEN COUNT(*) ELSE 1 END AS DECIMAL) AS DECIMAL(3,2)) AS icu_patient_vax" +
        "   FROM hospital_data A" +
        "   LEFT JOIN vax_data B" +
        "   ON A.patient_mrn = B.patient_mrn" +
        "   WHERE A.patient_status = 2";

        // Ventilator query
        ventilatorQueryString = "SELECT COUNT(*) AS patient_vent_count, " +
        "   CAST(CAST(CASE WHEN COUNT(*) > 0  THEN SUM(CASE WHEN B.vaccination_id is not null then 1 ELSE 0 END) ELSE 0 END AS DECIMAL) / CAST(CASE WHEN COUNT(*) > 0 THEN COUNT(*) ELSE 1 END AS DECIMAL) AS DECIMAL(3,2)) AS patient_vent_vax" +
        "   FROM hospital_data A" +
        "   LEFT JOIN vax_data B" +
        "   ON A.patient_mrn = B.patient_mrn" +
        "   WHERE A.patient_status = 3";

        List<Map<String,String>> accessMapList = Launcher.embeddedEngine.getPatientData(inPatientQueryString, ICUQueryString, ventilatorQueryString);
        responseString = gson.toJson(accessMapList);
        responseString = responseString.replace("},{", ",");
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getconfirmedcontacts/{mrn}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getConfirmedContacts(@PathParam("mrn") String patient_mrn) {
        
        String responseString = "{}";
        String queryString = "SELECT contact_mrn FROM contacts WHERE patient_mrn = '" + patient_mrn + "'";
        
        try {
            List<Map<String,String>> accessMapList = Launcher.embeddedEngine.getContactList(queryString);
            //generate a response
            responseString = gson.toJson(accessMapList);
            responseString = responseString.replace("{\"contact_mrn\":", "").replace("}", "");
            responseString = "{\"contactList\":" + responseString + "}";
        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getpossiblecontacts/{mrn}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPossibleContacts(@PathParam("mrn") String patient_mrn) {
        
        String responseString = "{}";
        String queryString = "SELECT A.patient_mrn, B.event_id FROM patient_events A " +
        "   INNER JOIN (SELECT event_id from patient_events WHERE patient_mrn = '" + patient_mrn + "') B " +
        "   ON A.event_id = B.event_id " +
        "   WHERE patient_mrn != '" + patient_mrn + "'"; // add where statement for own patient_mrn
        
        try {
            Map<String,List> contactList = Launcher.embeddedEngine.getEventContacts(queryString);
            //generate a response
            responseString = gson.toJson(contactList);
            responseString = "{\"contactList\": " + responseString.replace("{", "[").replace("}", "]") + "}";
        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/reset")
    @Produces(MediaType.APPLICATION_JSON)
    public Response reset() {
        String responseString = "{}";
        try {
            int droppedTables = Launcher.embeddedEngine.dropTables();
            if (droppedTables != -1) {
                Launcher.embeddedEngine.initDB();
                System.out.println("Embedded database re-initialized!");
            }
            System.out.println("Clearing 'zipList'...");
            Launcher.zipList.clear();
            Map<String,Integer> responseMap = new HashMap<>();
            responseMap.put("reset_status_code", 1);

            responseString = gson.toJson(responseMap);
            System.out.println("Successfully reset all data..");

        } catch (Exception ex) {

            Map<String,Integer> responseMap = new HashMap<>();
            responseMap.put("reset_status_code", 0);

            responseString = gson.toJson(responseMap);
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

}
