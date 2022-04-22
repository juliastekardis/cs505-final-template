package cs505finaltemplate.CEP;

import cs505finaltemplate.Launcher;
import io.siddhi.core.util.transport.InMemoryBroker;
import java.util.*;
import org.json.JSONArray;
import org.json.JSONObject;

public class OutputSubscriber implements InMemoryBroker.Subscriber {

    private String topic;

    public OutputSubscriber(String topic, String streamName) {
        this.topic = topic;
    }

    @Override
    public void onMessage(Object msg) {

        try {
            System.out.println("OUTPUT CEP EVENT: " + msg);
            //You will need to parse output and do other logic,
            //but this sticks the last output value in main
            System.out.println("Last CEP output: " + Launcher.lastCEPOutput);

            String stringJsonArray;
            JSONObject jsonObject;
            JSONArray jsonArray;
            // create HashMap from output (current) CEP event
            Launcher.counts_current_cep.clear();
            try {
                stringJsonArray = "{ array: " + msg + " }";
                jsonObject = new JSONObject(stringJsonArray);
                jsonArray = jsonObject.getJSONArray("array");
            } catch (Exception e) {
                stringJsonArray = "{ array: [" + msg + "] }";
                jsonObject = new JSONObject(stringJsonArray);
                jsonArray = jsonObject.getJSONArray("array");
            }
            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject row = jsonArray.getJSONObject(i).getJSONObject("event"); 
                Launcher.counts_current_cep.put(row.getString("zip_code"), (int)row.getDouble("count"));
            }
            System.out.println("current CEP output: " + Launcher.counts_current_cep);

            // create HashMap from last CEP event
            Launcher.counts_last_cep.clear()
            try {
                stringJsonArray = "{ array: " + Launcher.lastCEPOutput + " }";
                jsonObject = new JSONObject(stringJsonArray);
                jsonArray = jsonObject.getJSONArray("array");
            } catch (Exception e) {
                stringJsonArray = "{ array: [" + Launcher.lastCEPOutput + "] }";
                jsonObject = new JSONObject(stringJsonArray);
                jsonArray = jsonObject.getJSONArray("array");
            }
            try {
                for (int i = 0; i < jsonArray.length(); i++) {
                    JSONObject row = jsonArray.getJSONObject(i).getJSONObject("event"); 
                    Launcher.counts_last_cep.put(row.getString("zip_code"), (int)row.getDouble("count"));
                }
            } catch (Exception e) { }
            System.out.println("Last CEP output: " + Launcher.counts_last_cep);

            // compare current and last hashmaps, add to zipList
            List<String> zipList = new ArrayList<String>();
            Launcher.counts_current_cep.forEach((key, value) -> {
                if (Launcher.counts_last_cep.containsKey(key)) {
                    if (value >= Launcher.counts_last_cep.get(key) * 2) {
                        zipList.add(key);
                    }
                }
            });
            System.out.println("zipList: " + zipList);

            // update last CEP
            Launcher.lastCEPOutput = String.valueOf(msg);
            
            System.out.println("");

            String[] sstr = String.valueOf(msg).split(":");
            String[] outval = sstr[2].split("}");
            //Launcher.accessCount = Long.parseLong(outval[0]);

            //System.out.println("sstr: " + Arrays.toString(sstr));
            //System.out.println("outval: " + Arrays.toString(outval));
            //System.out.println("accessCount: " + String.valueOf(Launcher.accessCount));

        } catch(Exception ex) {
            ex.printStackTrace();
        }

    }

    @Override
    public String getTopic() {
        return topic;
    }

}
