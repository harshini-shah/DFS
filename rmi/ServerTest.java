package rmi;

import java.io.Console;
import java.net.InetSocketAddress;

class EchoServer implements Server {

    @ Override
    public String echo(String s) throws RMIException {
        return "Echo: " + s;
    }
}

public class ServerTest {
    public static void main(String[] args) {
        Server server = new EchoServer();
        InetSocketAddress address = new InetSocketAddress("localhost", 12345);
        Skeleton<Server> skeleton = new Skeleton<Server>(Server.class, server, address);
        try {
            skeleton.start();
        } catch (RMIException e) {
            e.printStackTrace();
        }
        
        // Console does not work in eclipse so using the standard IO
        Console console = System.console();
        System.out.println("Console is " + console);
        if (console != null) {
            console.readLine("Press any key to stop the skeleton");
        } 
        skeleton.stop();
    }
}