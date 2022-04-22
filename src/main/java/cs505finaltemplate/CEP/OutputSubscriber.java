package cs505finaltemplate.CEP;

import cs505finaltemplate.Launcher;
import io.siddhi.core.util.transport.InMemoryBroker;
import java.util.Arrays;
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
            //System.out.println("msg[0]: " + msg[0]);
            
            //Launcher.counts_by_zip.put(jsonObject.getJSONObject("event"))

            //You will need to parse output and do other logic,
            //but this sticks the last output value in main
            System.out.println("Last CEP output: " + Launcher.lastCEPOutput);

            String stringJsonArray = "{ array: " + msg + " }";
            JSONObject jsonObject = new JSONObject(stringJsonArray);
            JSONArray jsonArray = jsonObject.getJSONArray("array");
            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject row = jsonArray.getJSONObject(i);
                System.out.println("jsonArrat.getS: " + row.toString());
            }


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
