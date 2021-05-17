import java.io.File;
import java.io.IOException;

public class TwoPhaseProgram {
    public static void main(String[] args) {
        try {
            if (0 < args.length) {
                Thread coordinator = new Thread(new Coordinator(5000, args[0]));
                coordinator.start();
            } else {
                System.err.println("Invalid arguments count:" + args.length);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
