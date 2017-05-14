package rmi;

public interface Server {
    public String echo(String s) throws RMIException;
}
