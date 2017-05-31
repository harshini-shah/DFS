package rmi;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RMIListener<T> extends Thread {
    
    private Skeleton<T> skeleton;
    private Class<T> serverClass;
    private T server;
    private ServerSocket serverSocket;
    private ExecutorService pool;
    private boolean stop;
    private Exception exception;
    
    public RMIListener(Skeleton<T> skeleton, Class<T> serverClass, T server, ServerSocket socket) {
        this.skeleton = skeleton;
        this.serverClass = serverClass;
        this.server = server;
        this.serverSocket = socket;
        this.pool = Executors.newCachedThreadPool();
        this.stop = false;
        this.exception = null;
    }
    
    @Override
    public void run() {
        while (true) {
            try {
                Socket socket = serverSocket.accept();
                pool.execute(new RMIHandler<T>(serverClass, server, socket, skeleton));
            } catch (SocketException se) {
                break;
            } catch (IOException e) {
                if (!skeleton.listen_error(e)) {
                    exception = e;
                    stop = true;
                    break;
                }
            }
        }
        // All code below is to stop the listener and skeleton
        pool.shutdown();
        System.out.println(pool.isShutdown());
        
        try {
            pool.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
        
        skeleton.serverSocket = null;
        skeleton.listener = null;
        skeleton.stopped(exception);
    }
}
