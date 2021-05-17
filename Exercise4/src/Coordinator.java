import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

public class Coordinator implements Runnable {
    private File PATH;
    // A Java program for a Handler

    private String id;
    //initialize socket and input stream

    private ServerSocket server = null;
    private ServerChannel channel = null;
    private BlockingQueue<String> publicChannel;
    private final String IP = "127.0.0.1";
    private final int SERVER_PORT = 5000;
    //  private File PATH = new File("src/input.txt");
    //private File PATH;

    private long timeToWait;
    private long timeFinish;
    //String timeMaster=null;

    // N.B. see if containerProc can be a valid class to encapsulated the data-structure of socket messages.
    //private ContainerProcesses processes;


    public Coordinator(int port, String pathFile) throws IOException {
        server = new ServerSocket(port);
        this.channel = new ServerChannel();
        this.PATH = new File(pathFile);
        //this.processes = new ContainerProcesses();
        System.out.println("Coordinator started");
    }

    @Override
    public void run() {
        System.out.println("read data ");
        loadData(PATH);
        Scanner in_cli = new Scanner(System.in);
        boolean stop = false;
        String command = "";

        // reads message from client until "Over" is sent
        while (!stop) {
            try {
                Thread.sleep(1000);
                System.out.print("Command: ");
                String[] parser = in_cli.nextLine().trim().split(" ");
                command = parser[0].toLowerCase();
                // verify if revieved stop
                stop = command.equals("stop");
                switch (command) {
                    case "coordinator":
                        System.out.println(myId());
                        break;
                    case "set-value":
                        channel.sendAll("prepare " + parser[1]);
                        String votation = channel.verifyAll();
                        if (votation.equals("ok")) {
                            channel.sendAll("commit-value");
                        } else {
                            /**  need to decided if this decision of abort is right
                             *  or we have to do something else */
                            channel.sendAll("abort");
                            System.out.println(votation);
                        }
                        break;
                    case "rollback":
                        channel.sendAll(command + " " + parser[1]);
                        String votationRoll = channel.verifyAll();
                        if (votationRoll.equals("ok")) {
                            channel.sendAll("commit-rollback ");
                        } else {
                            /**  need to decided if this decision of abort is right
                             *  or we have to do something else */
                            channel.sendAll("abort");
                            System.out.println(votationRoll);
                        }
                        break;
                    case "add":
                        if (myId().equals(parser[1]) || channel.verifyPartecipant(parser[1])) {
                            System.out.println("node already in the system");
                        } else {
                            addPartecipant(parser[1]);
                            channel.sendTo("need-sync", parser[1]);
                        }
                        break;
                    case "remove":
                        if (!parser[1].equals(myId()) && channel.verifyPartecipant(parser[1])) {
                            channel.sendTo("stop", parser[1]);
                            channel.remove(parser[1]);
                        } else if (parser[1].equals(myId())) {
                            // send to all to start a new election
                            channel.sendAll("need-election " + myId());
                            String voteElection = channel.verifyAll();
                            if (voteElection.equals("ok")) {
                                System.out.println("start election");
                                channel.sendAll("start-election ");
                                String newId = channel.getElectionResult();
                                if (!newId.equals("")) {
                                    setId(newId);
                                    channel.sendTo("stop", newId);
                                    channel.remove(newId);
                                    channel.sendAll("end-election");
                                    System.out.println("Coordinator change in " + myId());
                                } else {
                                    System.out.println("problemi con elezione");
                                }
                            }
                        } else System.out.println("invalid id node");
                        break;
                    case "time-failure":
                        if (parser.length < 3) {
                            System.out.println("Error request");
                        } else {
                            if (!parser[1].equals(myId())) {
                                channel.sendTo(command + " " + parser[2], parser[1]);
                            } else {
                                timeToWait = Integer.parseInt(parser[2]) * 1000;
                                timeFinish = System.currentTimeMillis() + timeToWait;
                                System.out.println("COORDINATOR failure...wait until his recover ");
                                while (System.currentTimeMillis() < timeFinish) {
                                }
                                System.out.println("Coordinator restart to work");
                            }
                        }
                        break;
                    case "arbitrary-failure":
                        if (parser.length < 3) {
                            System.out.println("Missed one argument");
                            break;
                        } else channel.sendTo(command + " " + parser[2], parser[1]);
                        break;
                    case "stop":
                        channel.sendAll(command);
                        channel.closeAllConnection();
                        stop = true;
                        Thread.sleep(2000);
                        break;
                    case "history": // command only for debugging
                        channel.sendAll(command);
                        String checkHistory = channel.verifyHistory();
                        System.out.println(checkHistory);
                        break;
                    case "history-of":
                        channel.sendTo("history", parser[1]);
                        String historyOf = channel.recvFrom(parser[1]);
                        System.out.println("History of " + parser[1] + " : " + historyOf);
                        break;
                    case "clear-queue":
                        channel.sendAll("clear-queue");
                        break;
                }

            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }

        // LOAD DATA

    }

    private String myId() {
        return this.id;
    }

    public void loadData(File path) {
        this.publicChannel = new LinkedBlockingQueue();
        try {
            Scanner my_Scanner = new Scanner(path);
            boolean systemFlag = false;
            boolean stateFlag = false;
            while (my_Scanner.hasNext()) {
                // Creation of the partecipants
                String line = my_Scanner.nextLine();
                if (line.equals("#System")) {
                    systemFlag = true;
                } else if (line.equals("#State")) {
                    systemFlag = false;
                    stateFlag = true;
                } else if (systemFlag && !line.equals("")) {
                    String[] process = line.split(";");
                    String newId = process[0];
                    if (process.length > 1) {
                        setId(newId);
                    } else {
                        addPartecipant(newId);
                    }
                } else if (stateFlag && !line.equals("")) {
                    String history = line.split(";")[1].trim();
                    channel.sendHistory(history);
                }
            }
            // select the coordinator for the first time
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void addPartecipant(String id) throws IOException {
        Partecipant p = new Partecipant(IP, SERVER_PORT, id, publicChannel);
        Socket socketProcess = server.accept();
        //processes.addProces(socketProces, index);
        channel.addPartecipant(id, socketProcess);
        new Thread(p).start();
    }

    private void setId(String id) {
        this.id = id;
    }
}

