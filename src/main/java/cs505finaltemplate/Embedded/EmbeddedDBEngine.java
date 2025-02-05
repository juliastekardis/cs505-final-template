package cs505finaltemplate.Embedded;

import com.google.gson.reflect.TypeToken;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class EmbeddedDBEngine {

    private DataSource ds;

    public EmbeddedDBEngine() {

        try {
            //Name of database
            String databaseName = "myDatabase";

            //Driver needs to be identified in order to load the namespace in the JVM
            String dbDriver = "org.apache.derby.jdbc.EmbeddedDriver";
            Class.forName(dbDriver).newInstance();

            //Connection string pointing to a local file location
            String dbConnectionString = "jdbc:derby:memory:" + databaseName + ";create=true";
            ds = setupDataSource(dbConnectionString);

            /*
            if(!databaseExist(databaseName)) {
                System.out.println("No database, creating " + databaseName);
                initDB();
            } else {
                System.out.println("Database found, removing " + databaseName);
                delete(Paths.get(databaseName).toFile());
                System.out.println("Creating " + databaseName);
                initDB();
            }
             */

            initDB();


        }

        catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    public static DataSource setupDataSource(String connectURI) {
        //
        // First, we'll create a ConnectionFactory that the
        // pool will use to create Connections.
        // We'll use the DriverManagerConnectionFactory,
        // using the connect string passed in the command line
        // arguments.
        //
        ConnectionFactory connectionFactory = null;
        connectionFactory = new DriverManagerConnectionFactory(connectURI, null);


        //
        // Next we'll create the PoolableConnectionFactory, which wraps
        // the "real" Connections created by the ConnectionFactory with
        // the classes that implement the pooling functionality.
        //
        PoolableConnectionFactory poolableConnectionFactory =
                new PoolableConnectionFactory(connectionFactory, null);

        //
        // Now we'll need a ObjectPool that serves as the
        // actual pool of connections.
        //
        // We'll use a GenericObjectPool instance, although
        // any ObjectPool implementation will suffice.
        //
        ObjectPool<PoolableConnection> connectionPool =
                new GenericObjectPool<>(poolableConnectionFactory);

        // Set the factory's pool property to the owning pool
        poolableConnectionFactory.setPool(connectionPool);

        //
        // Finally, we create the PoolingDriver itself,
        // passing in the object pool we created.
        //
        PoolingDataSource<PoolableConnection> dataSource =
                new PoolingDataSource<>(connectionPool);

        return dataSource;
    }

    public void initDB() {
        System.out.println("Initializing embedded database...");

        String createRNode = "CREATE TABLE hospital_data" +
                "(" +
                "   hospital_id int," +
                "   patient_mrn varchar(255)," +
                "   patient_status smallint" +
                ")";

        try {
            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(createRNode);
                }
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }

        createRNode = "CREATE TABLE vax_data" +
                "(" +
                "   patient_mrn varchar(255), " +
                "   vaccination_id int" +
                ")";

        try {
            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(createRNode);
                }
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }

        createRNode = "CREATE TABLE contacts" +
                "(" +
                "   patient_mrn varchar(255), " +
                "   contact_mrn varchar(255)" +
                ")";

        try {
            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(createRNode);
                }
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }

        createRNode = "CREATE TABLE patient_events" +
                "(" +
                "   patient_mrn varchar(255), " +
                "   event_id varchar(255)" +
                ")";

        try {
            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(createRNode);
                }
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    void delete(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                delete(c);
        }
        if (!f.delete())
            throw new FileNotFoundException("Failed to delete file: " + f);
    }

    public int executeUpdate(String stmtString) {
        int result = -1;
        System.out.println(stmtString);
        try {
            Connection conn = ds.getConnection();
            try {
                Statement stmt = conn.createStatement();
                result = stmt.executeUpdate(stmtString);
                stmt.close();
            } catch (Exception e) {

                e.printStackTrace();
            } finally {
                conn.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return  result;
    }

    public int dropTables() {
        List<String> tableNames = new ArrayList<String>();
        tableNames.add("hospital_data");
        tableNames.add("vax_data");
        tableNames.add("contacts");
        tableNames.add("patient_events");
        int result = -1;
        try {
            Connection conn = ds.getConnection();
            try {
                String stmtString = null;
                for (String tableName : tableNames) {
                    try {
                        stmtString = "DROP TABLE " + tableName;

                        Statement stmt = conn.createStatement();

                        result = stmt.executeUpdate(stmtString);

                        stmt.close();
                        System.out.println("Successfully dropped table '" + tableName + "'");
                    } catch(Exception ex) {
                        ex.printStackTrace();
                    }
                    
                }

                
            } catch (Exception e) {

                e.printStackTrace();
            } finally {
                conn.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return result;
    }

    /*
    public boolean databaseExist(String databaseName)  {
        return Paths.get(databaseName).toFile().exists();
    }
    */
    public boolean databaseExist(String databaseName)  {
        boolean exist = false;
        try {

            if(!ds.getConnection().isClosed()) {
                exist = true;
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return exist;
    }

    public boolean tableExist(String tableName)  {
        boolean exist = false;

        ResultSet result;
        DatabaseMetaData metadata = null;

        try {
            metadata = ds.getConnection().getMetaData();
            result = metadata.getTables(null, null, tableName.toUpperCase(), null);

            if(result.next()) {
                exist = true;
            }
        } catch(java.sql.SQLException e) {
            e.printStackTrace();
        }

        catch(Exception ex) {
            ex.printStackTrace();
        }
        return exist;
    }

    public List<Map<String,String>> getPatientData(String inPatientQueryString, String ICUQueryString, String ventilatorQueryString) {
        List<Map<String,String>> accessMapList = null;
        try {

            accessMapList = new ArrayList<>();

            Type type = new TypeToken<Map<String, String>>(){}.getType();

            //String queryString = null;

            //fill in the query
            //queryString = "SELECT * FROM accesslog";

            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {

                    try(ResultSet rs = stmt.executeQuery(inPatientQueryString)) {

                        while (rs.next()) {
                            Map<String, String> accessMap = new HashMap<>();
                            accessMap.put("in_patient_count", rs.getString("in_patient_count"));
                            accessMap.put("in_patient_vax", rs.getString("in_patient_vax"));
                            accessMapList.add(accessMap);
                        }

                    }
                    try(ResultSet rs = stmt.executeQuery(ICUQueryString)) {

                        while (rs.next()) {
                            Map<String, String> accessMap = new HashMap<>();
                            accessMap.put("icu_patient_count", rs.getString("icu_patient_count"));
                            accessMap.put("icu_patient_vax", rs.getString("icu_patient_vax"));
                            accessMapList.add(accessMap);
                        }

                    }
                    try(ResultSet rs = stmt.executeQuery(ventilatorQueryString)) {

                        while (rs.next()) {
                            Map<String, String> accessMap = new HashMap<>();
                            accessMap.put("patient_vent_count", rs.getString("patient_vent_count"));
                            accessMap.put("patient_vent_vax", rs.getString("patient_vent_vax"));
                            accessMapList.add(accessMap);
                        }

                    }
                }
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }

        return accessMapList;
    }

    public List<Map<String,String>> getContactList(String queryString) {
        List<Map<String,String>> accessMapList = null;
        try {

            accessMapList = new ArrayList<>();

            Type type = new TypeToken<Map<String, String>>(){}.getType();

            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {

                    try(ResultSet rs = stmt.executeQuery(queryString)) {

                        while (rs.next()) {
                            Map<String, String> accessMap = new HashMap<>();
                            //accessMap.put("patient_mrn", rs.getString("patient_mrn")); // remove this later
                            accessMap.put("contact_mrn", rs.getString("contact_mrn"));
                            accessMapList.add(accessMap);
                        }

                    }
                }
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        List<Map<String, String>> accessMapListUnique = accessMapList.stream().distinct().collect(Collectors.toList());
        return accessMapListUnique;
    }

    public Map<String,List> getEventContacts(String queryString) {
        List<Map<String,String>> accessMapList = null;
        try {
            accessMapList = new ArrayList<>();

            Type type = new TypeToken<Map<String, String>>(){}.getType();

            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {

                    try(ResultSet rs = stmt.executeQuery(queryString)) {

                        while (rs.next()) {
                            Map<String, String> accessMap = new HashMap<>();
                            accessMap.put("event_id", rs.getString("event_id"));
                            accessMap.put("patient_mrn", rs.getString("patient_mrn"));
                            accessMapList.add(accessMap);
                        }

                    }
                }
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        List<Map<String, String>> accessMapListUnique = accessMapList.stream().distinct().collect(Collectors.toList());
        Map<String,List> contactList = new HashMap<>();
        for (Map<String,String> i : accessMapListUnique) {
            if (!contactList.containsKey(i.get("event_id"))) {
                contactList.put(i.get("event_id"), new ArrayList<>());
            }
            contactList.get(i.get("event_id")).add(i.get("patient_mrn"));
        }
        return contactList;
    }
}