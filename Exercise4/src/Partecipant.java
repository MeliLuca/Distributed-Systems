import javax.swing.text.Style;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Partecipant implements Runnable {
    private final String id;
    private List<String> history;
    private Socket socket = null;
    private BufferedReader input = null;
    private PrintWriter outServer = null;
    private BlockingQueue privateQueue;
    private BlockingQueue<String> publicQueue;
    private Runnable channel;
    private boolean stop = false;
    private boolean arbitrary = false;
    private boolean waitForMessage = false;
    private boolean outOfElection = false;
    private final int TIMEOUT_SYNC = 2000;
    private final int TIMEOUT_ELECTION = 2000;
    private String valueToCommit;
    private long timeToWait;
    private long timeFinish;
    private long timeArbitrary;

    public Partecipant(String address, int port, String id, BlockingQueue publicQueue) {
        this.id = id;
        this.history = new ArrayList<>();
        this.privateQueue = new LinkedBlockingQueue();
        this.publicQueue = publicQueue;
        // connection with the server
        try {
            this.socket = new Socket(address, port);
            // takes input from the handler socket
            input = new BufferedReader(
                    new InputStreamReader(socket.getInputStream()));
            // sends output to the socket
            outServer = new PrintWriter(socket.getOutputStream(), true);

        } catch (UnknownHostException u) {
            System.out.println(u);
        } catch (IOException i) {
            System.out.println(i);
        }

        // Start deamon to communicate with other partecipants
        //new Thread(channel).start();
    }

    @Override
    public void run() {
        // string to read message from input
        while (!stop) {
            String[] parser;
            if (arbitrary) {
                arbitrary = timeFinish > System.currentTimeMillis();
            }
            try {
                checkQueue();
                parser = readFromServer();
                String command = parser[0];
                // prova per vedere se funziona
                switch (command) {
                    case "init_history":
                        String[] history_val = parser[1].trim().split(",");
                        insertHistory(history_val);
                        // check if it's correct to get the initial time of coordinato
                        break;
                    case "prepare":
                    case "need-election":
                        prepareCommit(parser[1]);
                        sendOk();
                        break;
                    case "commit-value":
                        if (valueToCommit != "") {
                            addHistory(valueToCommit);
                        } else {
                            askForSync(TIMEOUT_SYNC);
                        }
                        break;
                    case "abort":
                        abortCommit();
                        break;
                    case "commit-rollback":
                        if (valueToCommit != "") {
                            int end_index = Integer.parseInt(valueToCommit);
                            rollbackHistory(0, end_index);
                        } else {
                            askForSync(TIMEOUT_SYNC);
                        }
                        break;
                    case "rollback":
                        String rollbackRequest = parser[1];
                        int endIndex = 0;
                        boolean flag = false;
                        while (endIndex < getHistorySize() && !flag) {
                            if (rollbackRequest.equals(getValue(endIndex))) {
                                flag = true;
                            }
                            endIndex++;
                        }
                        if (flag) {
                            prepareCommit("" + endIndex);
                            sendOk();
                        } else writeToServer("ABORT: Value not found");
                        break;
                    case "start-election":
                        outOfElection = false;
                        writeToQueue("all el " + getId());
                        long maxTimeMillis = System.currentTimeMillis() + TIMEOUT_ELECTION;
                        while (System.currentTimeMillis() < maxTimeMillis && !outOfElection) {
                            checkElection();
                        }
                        if (outOfElection) {
                            waitForEnd();
                        } else {
                            publicQueue.clear();
                            writeToServer(getId());
                        }
                        break;
                    case "need-sync":
                        askForSync(TIMEOUT_SYNC);
                        while (input.ready()) {
                            input.readLine();
                        }
                        break;
                    case "history":
                        String listVal = printHistory();
                        writeToServer(listVal);
                        break;
                    case "stop":
                        stop = true;
                        socket.close();
                        input.close();
                        outServer.close();
                        break;
                    case "time-failure":
                        Integer secondsTime = Integer.parseInt(parser[1]);
                        timeToWait = secondsTime * 1000;
                        Thread.sleep(timeToWait);
                        askForSync(TIMEOUT_SYNC);
                        // clear the channel to avoid useless command
                        while (input.ready()) {
                            input.readLine();
                        }
                        break;
                    case "arbitrary-failure":
                        long secondsArbitrary = Integer.parseInt(parser[1]);
                        arbitrary = true;
                        timeArbitrary = secondsArbitrary * 1000;
                        timeFinish = System.currentTimeMillis() + timeArbitrary;
                        break;
                    case "clear-queue":
                        publicQueue.clear();
                        break;
                }
                if (needToPrint(command)) {
                    System.out.println(" #" + getId() + " : " + printHistory());
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean needToPrint(String command) {
        return (command.equals("commit-value") || command.equals("commit-rollback") || command.equals("start-election")
                || command.equals("arbitrary-failure") || command.equals("time-failure") );
    }

    /**
     * Start methods for handling the QUEUE
     */
    private void checkElection() {
        String tmpCheck = publicQueue.peek();
        boolean isElection = tmpCheck != null && tmpCheck.split(" ")[1].equals("el");
        String sender = isElection? tmpCheck.split(" ")[2] : "";
        if (!isMine(sender.trim()) && isElection) {
            try {
                String queueReq = publicQueue.remove();
                parserPublicQueue(queueReq);
            } catch (NullPointerException | InterruptedException e) {
            }
        }

    }

    private String waitResponse(int timeout) {
        boolean recieved = false;
        String result = "";
        long maxTimeMillis = System.currentTimeMillis() + timeout;
        while (!recieved && System.currentTimeMillis() < maxTimeMillis) {
            String tmp = publicQueue.peek();
            String address = tmp != null ? tmp.split(" ")[0] : "";
            if (isMine(address)) {
                try {
                    String queueReq = publicQueue.remove();
                    result = queueReq.split(" ")[1];
                    recieved = true;
                } catch (NullPointerException | NoSuchElementException e) {
                    recieved = true;
                }
            }
        }
        return result;
    }


    private void checkQueue() {
        if (!publicQueue.isEmpty()) {
            try {
                String request = publicQueue.element();
                String address = request.split(" ")[0];
                if (!waitForMessage && isForAll(address)) {
                    String queueReq = publicQueue.remove();
                    parserPublicQueue(queueReq);
                }
            } catch (NullPointerException | NoSuchElementException | InterruptedException e) {
            }
        }
    }

    private void askForSync(int timeout) throws InterruptedException {
        writeToQueue("all sync " + getId());
        waitForMessage = true;
        String respRetrieved = waitResponse(timeout);
        if (respRetrieved.split(",").length < 1) {
            askForSync(timeout);
        } else {
            waitForMessage = false;
            if (!respRetrieved.equals("")) {
                resetHistory(respRetrieved.split(","));
                publicQueue.clear();
            }
            else {
                //System.out.println("FAILURE SYNC");
                askForSync(timeout);
            }
        }
    }

    // When enter here the element in the Queue was removed !
    // The request are written [ adress , command, sender ]
    private void parserPublicQueue(String queueReq) throws InterruptedException {
        String[] token = queueReq.split(" ");
        String req = token[1];
        String recvr = token[2];
        switch (req) {
            case "sync":
                try {
                    writeToQueue(recvr + " " + printHistory() + " " + getId());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                break;
            case "el":
                if (!biggerId(recvr)) {
                    try {
                        writeToQueue("all el " + recvr);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    outOfElection = true;
                }
                Thread.sleep(500);
                break;
        }
    }

    /**
     * END METHODS FOR HANDLER THE QUEUE
     */

    private void waitForEnd() throws IOException {
        String end = input.readLine();
        if (!end.equals("end-election")) {
            waitForEnd();
        }
    }

    private boolean biggerId(String valueToCommit) {
        int other = Integer.parseInt(String.valueOf(valueToCommit.charAt(1)));
        int mine = Integer.parseInt(String.valueOf(getId().charAt(1)));
        return mine > other;
    }

    private void sendOk() {
        String respToServ = arbitrary ? "ABORT " : "ok";
        writeToServer(respToServ);
    }

    private void prepareCommit(String s) {
        this.valueToCommit = s;
    }

    private void abortCommit() {
        this.valueToCommit = "";
    }


    private void writeToServer(String text) {
        outServer.println(text);
    }

    private String[] readFromServer() throws IOException {
        if (input.ready()) return input.readLine().split(" ");
        else return new String[]{""};
    }


    private String printHistory() {
        String res = "";
        for (String el : getHistory()) {
            res += el + ",";
        }
        return res;
    }

    private void resetHistory(String[] history_val) {
        history.clear();
        insertHistory(history_val);
    }

    private void insertHistory(String[] history_val) {
        history.addAll(Arrays.asList(history_val));
    }

    private void rollbackHistory(int i, int endIndex) {
        this.history = this.history.subList(i, endIndex);
    }

    private int getHistorySize() {
        return this.history.size();
    }


    private boolean isForAll(String s) {
        return s.equals("all");
    }

    private boolean isMine(String request) {
        return request.equals(getId());
    }


    private void writeToQueue(String s) throws InterruptedException {
        publicQueue.put(s);
    }

    public String getId() {
        return id;
    }

    public String getValue(int i) {
        return history.get(i);
    }

    public List<String> getHistory() {
        return history;
    }

    public void addHistory(String val) {
        this.history.add(val);
    }
}
