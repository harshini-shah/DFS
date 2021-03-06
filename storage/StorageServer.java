package storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.FileChannel;
import java.util.Iterator;

import common.Path;
import naming.Registration;
import rmi.RMIException;
import rmi.Stub;

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
	private Throwable stoppedCause;
	private static final int CHUNK_SIZE = 1000000; // Chunk size when files are copied
	
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
        if (root == null) {
            throw new NullPointerException("Root cannot be null");
        }
        this.root = root;
        
        storageSkeleton = new StorageSkeleton<Storage>(Storage.class, this, new InetSocketAddress(client_port));
        commandSkeleton = new CommandSkeleton<Command>(Command.class, this, new InetSocketAddress(command_port));
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
        if (root == null) {
            throw new NullPointerException("Root cannot be null");
        }
        this.root = root;
        
        storageSkeleton = new StorageSkeleton<Storage>(Storage.class, this);
        commandSkeleton = new CommandSkeleton<Command>(Command.class, this);
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
    	commandSkeleton.start();
    	storageSkeleton.start();
    	
    	//Returns relative paths of all files in root
    	Path currentFiles[] = Path.list(root);
    	
    	//register returns redundant files
    	Path[] redundantFiles = naming_server.register(Stub.create(Storage.class, storageSkeleton, hostname),
    											Stub.create(Command.class, commandSkeleton, hostname), currentFiles);
    	for(Path path: redundantFiles) {
    	    delete(path);
    	}
    	
    	removeEmptyDirectories(root);
    }

    /** Stops the storage server.
    
        <p>
        The server should not be restarted.
     */
    public void stop()
    {
    	//Stop both services
        if (!commandSkeleton.stopped) {
            commandSkeleton.stop();
        }
        
        if (!storageSkeleton.stopped) {
            storageSkeleton.stop();
        }
        
        //Call stopped() when shutting down
        stopped(null);
    }

    /** Called when the storage server has shut down.
    
        @param cause The cause for the shutdown, if any, or <code>null</code> if
                     the server was shut down by the user's request.
     */
    protected void stopped(Throwable cause)
    {
    	System.out.println("Storage server has been stopped");
        if (cause != null) 
        { 
            cause.printStackTrace(); 
        }
    }

    public void setStoppedCause(Throwable stoppedCause)
	{
		this.stoppedCause = stoppedCause;
	}
	
	public void removeEmptyDirectories(File dir)
	{
	    // Return if the file is a directory
		if(!dir.isDirectory()) {
		    return;
		}
		
		for(File subDir: dir.listFiles()) {
	          removeEmptyDirectories(subDir);
		}
		
		if (!dir.equals(root) && dir.listFiles().length == 0) {
		    dir.delete();
		}
	}

	// The following methods are documented in Storage.java.
    @Override
    public synchronized long size(Path file) throws FileNotFoundException
    {
        File f = file.toFile(root);
        
        if(!f.isFile()) {
            throw new FileNotFoundException("File does not exist or is not a normal file");
        }
        
        return f.length();
    }

    @Override
    public synchronized byte[] read(Path file, long offset, int length)
        throws FileNotFoundException, IOException, IndexOutOfBoundsException
    {
    	File f = file.toFile(root);
            	
        if (!f.exists() || !f.isFile()) {
            throw new FileNotFoundException("File does not exist or is not a normal file");
        }
        
        if(offset + length > f.length() || offset < 0 || length < 0) {
            throw new IndexOutOfBoundsException("The offset and length do not conform to the file size");            
        }
        
        FileInputStream fis = new FileInputStream(f);
        byte[] byteStream = new byte[length];
        fis.skip(offset);
        fis.read(byteStream);
        
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
        
        //Path can't be root itself or already exists
        if(f.equals(root) || f.exists()) {
            return false;
        }
        
        //Returns new PathIterator to ensure all the parent folders exist
        Iterator<String> iter = file.iterator();
        File path = root;
        
        while (iter.hasNext()) {
            File subDirectory = new File(path, iter.next());
            if (iter.hasNext()) {
                // The current subDirectory is one of the parents
                if (!subDirectory.exists()) {
                    // create required subdirectory
                    if (subDirectory.mkdir()) {
                        path = subDirectory;
                    } else {
                        return false;
                    }
                } else if (!subDirectory.isDirectory()) {
                    // One of the subdirectories specified is a file - error
                    return false;
                } else {
                    // Subdirectory already exists
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
        
        if(f.equals(root) || !f.exists()) {
            return false;
        }
        
        boolean isDeleted = false;
        
        if(!f.isDirectory()) {
            isDeleted = f.delete();
        } else {
            isDeleted = deleteRecursive(f);
        }
        
        //Delete empty parent directories
        if(isDeleted) {
            File parent = f.getParentFile();
            while (parent.isDirectory() && parent.listFiles().length == 0 && !parent.equals(root)) {
                parent.delete();
                parent = parent.getParentFile();
            }
        }

        return isDeleted;
    }

    @Override
    public synchronized boolean copy(Path file, Storage server)
        throws RMIException, FileNotFoundException, IOException
    {
        // Check that server is not null - if it is null, throw NullPointerException
        if (server == null) {
            throw new NullPointerException("Storage server cannot be null");
        }
        
        // Check that the path is not null - throw NullPointerException if required
        if (file == null) {
            throw new NullPointerException("Path of file to be copied cannot be null");
        }
     
        // Copy the file in chunks of 1 MB        
        long size = server.size(file);
        long offset = 0;
        byte[] chunk = new byte[CHUNK_SIZE];
        
        // Create the file
        create(file);
        
        while ((offset + CHUNK_SIZE) < size) {
            chunk = server.read(file, offset, CHUNK_SIZE);
            write(file, offset, chunk);
            offset += CHUNK_SIZE;
        }
        
        // Copy left over bytes
        int finalChunkSize = (int) (size - offset);
        byte[] finalChunk = new byte[finalChunkSize];
        finalChunk = server.read(file, offset, finalChunkSize);
        write(file, offset, finalChunk);
        
        return true;
    }
}