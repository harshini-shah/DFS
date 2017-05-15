package rmi;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.Socket;

/*
 * This method establishes the connection to the skeleton specified and can be used
 * to communicate in the future. The actual RPC call is made here.
 */
public class StubProxyHandler implements InvocationHandler, Serializable {
    
    protected Class<?> remoteClass;
    protected InetSocketAddress address;
    
    public StubProxyHandler(Class<?> remoteClass, InetSocketAddress address) {
        this.remoteClass = remoteClass;
        this.address = address;
    }
    
    @ Override
    public Object invoke(Object proxy, Method method, Object [] args) throws Throwable {        
        if (method.equals(Object.class.getMethod("equals", Object.class))) {
            Object target = args[0];
            
            // Returns false if no argument is provided to the equals method
            // or if it isn't a stub 
            if (target == null || !Proxy.isProxyClass(target.getClass())) {
                return false;
            }
            
            // Checks that the target InvocationHandler is of type StubProxyHandler
            InvocationHandler targetHandler = Proxy.getInvocationHandler(target);
            if (!(targetHandler instanceof StubProxyHandler)) {
                return false;
            }
            
            // Get the details of the target StubProxyHandler and check if it 
            // is equal to the correct one
            StubProxyHandler targetStubProxyHandler = (StubProxyHandler)targetHandler;
            if (targetStubProxyHandler.address == null ^ this.address == null) {
                return false;
            }
            
            if (targetStubProxyHandler != null) {
                if (targetStubProxyHandler.address.getPort() != this.address.getPort() || 
                        !targetStubProxyHandler.address.getAddress().equals(this.address.getAddress())) {
                    return false;
                }
            }
            
            // Check that the remoteClass for both is the same
            if (!targetStubProxyHandler.remoteClass.equals(this.remoteClass)) {
                return false;
            }
            
            // If all the conditions are satisfied, then return true 
            return true;
        } else if (method.equals(Object.class.getMethod("hashCode"))) {
            // Return the hashcode of this object
            return this.remoteClass.hashCode() ^ this.address.hashCode();
        } else if (method.equals(Object.class.getMethod("toString"))) {
            // Return the string representation of this object
            return this.remoteClass.toString() + this.address.toString(); 
        } else {
            // Make the remote procedure call
            boolean success = false;
            Socket socket = null;
            Object response = null;
            
            // Try setting up the connection with the skeleton specified
            try {
                // Establish the connection, get input and output IO streams
                socket = new Socket(address.getAddress(), address.getPort());
                ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                outputStream.flush();
                ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());

                // Send the arguments of the function
                outputStream.writeObject(method.getName());
                if (args != null) {
                    int numParameters = args.length;
                    outputStream.writeObject(numParameters);
                    Class<?>[] parameterTypes = method.getParameterTypes();
                    for (int i = 0; i < numParameters; i++) {
                        outputStream.writeObject(parameterTypes[i]);
                        outputStream.writeObject(args[i]);
                    }
                } else {
                    outputStream.writeObject((int)0);
                }
                outputStream.flush();

                // Receive result
                success = (Boolean) inputStream.readObject();
                response = inputStream.readObject();
            } catch (Exception e) {
                // If unable to set up connection
                throw new RMIException(e);
            } finally {
                // Close the socket 
                try {
                    if (socket != null && !socket.isClosed()) {
                        socket.close();
                    }
                } catch (IOException e) {
                    throw new RMIException(e);
                }
            }
            
            // If not a success (no connection) then throw an exception
            if (!success) {
                Throwable exception = (Throwable)response;
                throw exception.getCause();
            }
            
            // If everything worked, return the response
            return response;
            
        }        
    }
    
    public int hashCode(){
        return this.address.hashCode() ^ this.remoteClass.hashCode();

    }
    public String toString(){   
        System.out.println("Name of the remote interface is "+ this.remoteClass.getName());
        System.out.println("Remote address: hostname: " + this.address.getHostName()+" port:" +this.address.getPort()); 
        return "";
    }
}
