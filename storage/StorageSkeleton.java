/*
 * Sets up the skeleton for the storage server to talk to the client.
 */

package storage;

import java.net.InetSocketAddress;
import rmi.*;

public class StorageSkeleton<T> extends Skeleton<T>
{
	private StorageServer storageServer;
	public boolean stopped;
	
	//Creates a Storage Skeleton similar to the two cases in Skeleton
	/*
	 * Used when initial server address is not known
	 */
	public StorageSkeleton(Class<T> c, T server)
	{
		super(c, server);
		
		stopped = false;
		storageServer = (StorageServer) server;
	}
	
	/*
	 * Used when server address is known and port number is significant
	 */
	public StorageSkeleton(Class<T> c, T server, InetSocketAddress address)
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
