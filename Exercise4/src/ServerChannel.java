import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ServerChannel {
    Map<String, Socket> socketMap;
    Map<String, PrintWriter> outMap;
    Map<String, BufferedReader> inputMap;
    private final int TIMEOUT_MILLIS = 2000;


    public ServerChannel() {
        this.socketMap = new HashMap<>();
        this.outMap = new HashMap<>();
        this.inputMap = new HashMap<>();
    }


    public void addPartecipant(String id, Socket socketProces) throws IOException {
        socketMap.put(id, socketProces);
        outMap.put(id, new PrintWriter(socketProces.getOutputStream(), true));
        inputMap.put(id, new BufferedReader(new InputStreamReader(socketProces.getInputStream())));
    }

    public void sendAll(String text) {
        outMap.forEach((k, writer) -> writer.println(text));
    }

    public void sendHistory(String history) {
        sendAll("init_history " + history);
    }

    public void sendTo(String command, String sender) {
        //System.out.println(sender);
        try{
            outMap.get(sender).println(command);

        }catch(NullPointerException e){
            e.printStackTrace();
        }
    }

    public String sendPrepare() throws IOException {
        sendAll("prepare");
        return verifyAll();
    }

    public String verifyAll() throws IOException {
        String result = "";
        for (BufferedReader partecipantChan : inputMap.values()) {
            long maxTimeMillis = System.currentTimeMillis() + TIMEOUT_MILLIS;
            boolean isPartReady = false;
            while (System.currentTimeMillis() < maxTimeMillis && !isPartReady) {
                if (partecipantChan.ready()) {
                    isPartReady = true;
                    String msg = partecipantChan.readLine();
                    if (msg.contains("ABORT")) result = msg;
                }
            }
            if (!isPartReady) {
                result = "ABORT: partecipant not available";
                break;
            }
        }
        return result.equals("") ? "ok" : result;
    }

    public String verifyHistory() throws IOException {
        String last_hist = "";
        boolean correct = true;
        for (BufferedReader partecipantChan : inputMap.values()) {
            if (last_hist.equals("")) last_hist = partecipantChan.readLine();
            else if (!last_hist.equals(partecipantChan.readLine())) correct = false;
        }
        return correct ? last_hist : "DIFFERENT values";
    }

    public String recvFrom(String id) {
        try {
            return inputMap.get(id).readLine();
        } catch (IOException e) {
            e.printStackTrace();
            return ("history retrieve of " + id + " didn't work");
        }
    }

    public void closeAllConnection() throws IOException {
        socketMap.forEach((k, s) -> {
            try {
                s.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        inputMap.forEach((k, in) -> {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        outMap.forEach((k, w) -> {
            w.close();
        });
    }

    public void remove(String s) throws IOException {
        socketMap.get(s).close();
        socketMap.remove(s);
        inputMap.get(s).close();
        inputMap.remove(s);
        outMap.get(s).close();
        outMap.remove(s);
        System.out.println(" #" + s + " removed");
    }

    public String getElectionResult() throws IOException {
        String electionId = "";
        boolean winner = false;
        while (!winner) {
            for (BufferedReader partChan : inputMap.values()) {
                if (partChan.ready()) {
                    winner = true;
                    electionId = partChan.readLine();
                }
            }
        }
        return electionId;
    }

    public boolean verifyPartecipant(String id) {
        return (socketMap.getOrDefault(id, null) != null);

    }
}
