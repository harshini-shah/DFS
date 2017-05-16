package storage;

import java.io.*;
import java.util.Iterator;
import java.net.*;
import java.nio.channels.FileChannel;

import common.*;
import rmi.*;
import naming.*;
import common.*;

/** Storage server.

    <p>
    Storage servers respond to client file access requests. The files accessible
    through a storage server are those accessible under a given directory of the
    local filesystem.
 */
public class StorageServer implements Storage, Command
{
	private final File root; //root filesystem for this Storage server
	private CommandSkeleton<Command> commandSkeleton;
	private StorageSkeleton<Storage> storageSkeleton;
	Throwable stoppedCause;
	
	public void setStoppedCause(Throwable stoppedCause)
	{
		this.stoppedCause = stoppedCause;
	}

	/** Creates a storage server, given a directory on the local filesystem, and
        ports to use for the client and command interfaces.

        <p>
        The ports may have to be specified if the storage server is running
        behind a firewall, and specific ports are open.

        @param root Directory on the local filesystem. The contents of this
                    directory will be accessible through the storage server.
        @param client_port Port to use for the client interface, or zero if the
                           system should decide the port.
        @param command_port Port to use for the command interface, or zero if
                            the system should decide the port.
        @throws NullPointerException If <code>root</code> is <code>null</code>.
    */
    public StorageServer(File root, int client_port, int command_port)
    {
        if(root == null)
        	throw new NullPointerException("Root was null");
        this.root = root;
        
        if(client_port == 0)
        	storageSkeleton = new StorageSkeleton(Storage.class, this);
        else
        	storageSkeleton = new StorageSkeleton(Storage.class, this, new InetSocketAddress(client_port));
        
        if(command_port == 0)
        	commandSkeleton = new CommandSkeleton(Command.class, this);
        else
        	commandSkeleton = new CommandSkeleton(Command.class, this, new InetSocketAddress(command_port));
    }

    /** Creates a storage server, given a directory on the local filesystem.

        <p>
        This constructor is equivalent to
        <code>StorageServer(root, 0, 0)</code>. The system picks the ports on
        which the interfaces are made available.

        @param root Directory on the local filesystem. The contents of this
                    directory will be accessible through the storage server.
        @throws NullPointerException If <code>root</code> is <code>null</code>.
     */
    public StorageServer(File root)
    {
        this(root, 0, 0);
    }

    /** Starts the storage server and registers it with the given naming
        server.

        @param hostname The externally-routable hostname of the local host on
                        which the storage server is running. This is used to
                        ensure that the stub which is provided to the naming
                        server by the <code>start</code> method carries the
                        externally visible hostname or address of this storage
                        server.
        @param naming_server Remote interface for the naming server with which
                             the storage server is to register.
        @throws UnknownHostException If a stub cannot be created for the storage
                                     server because a valid address has not been
                                     assigned.
        @throws FileNotFoundException If the directory with which the server was
                                      created does not exist or is in fact a
                                      file.
        @throws RMIException If the storage server cannot be started, or if it
                             cannot be registered.
     */
    public synchronized void start(String hostname, Registration naming_server)
        throws RMIException, UnknownHostException, FileNotFoundException
    {
        throw new UnsupportedOperationException("not implemented");
    }

    /** Stops the storage server.

        <p>
        The server should not be restarted.
     */
    public void stop()
    {
        throw new UnsupportedOperationException("not implemented");
    }

    /** Called when the storage server has shut down.

        @param cause The cause for the shutdown, if any, or <code>null</code> if
                     the server was shut down by the user's request.
     */
    protected void stopped(Throwable cause)
    {
    	System.out.println("Storage server stopped");
        if (cause != null) 
        { 
            cause.printStackTrace(); 
        }
    }

    // The following methods are documented in Storage.java.
    @Override
    public synchronized long size(Path file) throws FileNotFoundException
    {
        File f = file.toFile(root);  //why root?
        
        if(!f.isFile())
        	throw new FileNotFoundException("File does not exist or is not a normal file");
        
        return f.length();
    }

    @Override
    public synchronized byte[] read(Path file, long offset, int length)
        throws FileNotFoundException, IOException, IndexOutOfBoundsException
    {
    	File f = file.toFile(root);
        
        if(!f.isFile())
        	throw new FileNotFoundException("File does not exist or is not a normal file");
        if(offset + length > f.length() || offset < 0 || length < 0)
        	throw new IndexOutOfBoundsException("The offset and length do not conform to the file size");
        
        FileInputStream fis = new FileInputStream(f);
        byte[] byteStream = new byte[length];
        fis.skip(offset);
        fis.read(byteStream);   // will throw IOException
        
        if (fis != null) {
            fis.close();
        }
                
        return byteStream;
    }

    @Override
    public synchronized void write(Path file, long offset, byte[] data)
        throws FileNotFoundException, IOException
    {
    	File f = file.toFile(root);
        
        if (!f.isFile())
        	throw new FileNotFoundException("File does not exist or is not a normal file");
        if (offset < 0)
        	throw new IndexOutOfBoundsException();
        
        //Appending to a file with an offset does not make sense
        FileOutputStream fos = new FileOutputStream(f, false);
        FileChannel ch = fos.getChannel();
        ch.position(offset);
        fos.write(data);
        
        if (fos != null) {
            fos.close();
        }
    }

    // The following methods are documented in Command.java.
    @Override
    public synchronized boolean create(Path file)
    {
        File f = file.toFile(root);
        
        //Path can't be root itself
        if(f.equals(root))
        	return false;
        
        //Returns new PathIterator(impl. iterator) from Path.java
        Iterator<String> iter = file.iterator();
        File path = root;
        
        while (iter.hasNext()) {
            File subDirectory = new File(path, iter.next());    // creates subdirectory 
            if (iter.hasNext()) {
                if (!subDirectory.exists()) {
                    if (subDirectory.mkdir()) {
                        path = subDirectory;
                    } else {
                        return false;
                    }
                } else if (!subDirectory.isDirectory()) {
                    return false;
                } else {
                    path = subDirectory;
                }
            } else {
                // Create the file which is the last component of the path
                try {
                    return subDirectory.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        
        //If all fails
		throw new Error("Create() failed"); 
    }
    
    
    public boolean deleteRecursive(File file)
    {
    	for(File child: file.listFiles())
    	{
    		if(child.isFile())
    		{
    			if(!child.delete())
    				return false;
    		}
    		else
    		{
    			if(!deleteRecursive(child))
    				return false;
    		}
    	}
    	return file.delete();
    }

    @Override
    public synchronized boolean delete(Path path)
    {
        File f = path.toFile(root);
        if(f.equals(root))
        	return false;
        boolean isDeleted;
        
        if(!f.isDirectory())
        	isDeleted = f.delete();
        else
        	isDeleted = deleteRecursive(f);
        
        //Delete empty parent directories
        if(isDeleted)
        {
        	File parent = f.getParentFile();
        	while(parent.isDirectory() && (parent.listFiles().length == 0) && !parent.equals(root))
        	{
        		parent.delete();
        		parent = parent.getParentFile();
        	}
        }
        return isDeleted;
    }

//    @Override
//    public synchronized boolean copy(Path file, Storage server)
//        throws RMIException, FileNotFoundException, IOException
//    {
//        if(server == null || file == null)
//        	throw new NullPointerException();
//        
//        File f = file.toFile(root);
//        //Deletion step?
//        long fileSize = server.size(file);
//        create(file);
//        byte[] byteStream;
//        
//        long offset = 0;
//        while(offset < fileSize)
//        {
//        	//read in int size chunks
//        	int length = (int) Math.min(Integer.MAX_VALUE, fileSize - offset);
//        	byteStream = server.read(file, offset, length);
//        	write(file, offset, byteStream);
//        	offset += length;
//        }
//        return true;
//    }
}
