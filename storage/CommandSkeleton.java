package storage;

import java.net.InetSocketAddress;
import rmi.*;

public class CommandSkeleton<T> extends Skeleton<T>
{
	private StorageServer storageServer;
	boolean stopped;
	
	public CommandSkeleton(Class<T> c, T server)
	{
		super(c, server);
		stopped = false;
		storageServer = (StorageServer) server;
	}
	
	public CommandSkeleton(Class<T> c, T server, InetSocketAddress address)
	{
		super(c, server, address);
		stopped = false;
		storageServer = (StorageServer) server;
	}
	
	@Override
	public void stopped(Throwable cause)
	{
		this.stopped = true;
		if (cause != null)
			this.storageServer.setStoppedCause(cause);
		storageServer.stop();
	}
}
