package rmi;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.net.Socket;
import java.util.ArrayList;

public class RMIHandler<T> implements Runnable {

    protected final Class<T> serverClass;
    protected final T server;
    protected final Socket socket;
    protected final Skeleton<T> skeleton;

    public RMIHandler(Class<T> serverClass, T server, Socket socket, Skeleton<T> skeleton) {
        this.serverClass = serverClass;
        this.server = server;
        this.socket = socket;
        this.skeleton = skeleton;
    }
    
    @ Override
    public void run() {
        try {
            ObjectOutputStream outputStream = new ObjectOutputStream((socket.getOutputStream()));
            outputStream.flush();
            ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());

            // Get the call information
            String methodName = (String) inputStream.readObject();
            int numParameters = (Integer) inputStream.readObject();
            ArrayList<Class<?>> parameterTypes = new ArrayList<Class<?>>();
            ArrayList<Object> parameterValues = new ArrayList<Object>();
            
            for (int i = 0; i < numParameters; i++) {
               parameterTypes.add((Class<?>) inputStream.readObject()); 
               parameterValues.add(inputStream.readObject());
            }

            // Make the procedure call (local)
            Object result = null;
            boolean success = true;
            try {
                Method method = serverClass.getMethod(methodName,
                        parameterTypes.toArray(new Class<?>[numParameters]));
                result = method.invoke(server, parameterValues.toArray());
            } catch (NoSuchMethodException e) {
                success = false;
                result = new Exception(new RMIException(e));
            } catch (Exception e) {
                success = false;
                result = e;
            }

            // Send back result/exception
            outputStream.writeObject(success);
            outputStream.writeObject(result);
            outputStream.flush();

        } catch (Exception e) {
            skeleton.service_error(new RMIException("Handler Exception", e));
        } 
    }
}
