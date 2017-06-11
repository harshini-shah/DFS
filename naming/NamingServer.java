package naming;

import java.io.*;
import java.net.*;
import java.util.*;

import rmi.*;
import common.*;
import storage.*;
import java.util.concurrent.ThreadLocalRandom;


/** Naming server.

    <p>
    Each instance of the filesystem is centered on a single naming server. The
    naming server maintains the filesystem directory tree. It does not store any
    file data - this is done by separate storage servers. The primary purpose of
    the naming server is to map each file name (path) to the storage server
    which hosts the file's contents.

    <p>
    The naming server provides two interfaces, <code>Service</code> and
    <code>Registration</code>, which are accessible through RMI. Storage servers
    use the <code>Registration</code> interface to inform the naming server of
    their existence. Clients use the <code>Service</code> interface to perform
    most filesystem operations. The documentation accompanying these interfaces
    provides details on the methods supported.

    <p>
    Stubs for accessing the naming server must typically be created by directly
    specifying the remote network address. To make this possible, the client and
    registration interfaces are available at well-known ports defined in
    <code>NamingStubs</code>.
 */
public class NamingServer implements Service, Registration
{
    FileTree fileTree;
    Map<Storage, Command> serverToClientStubMapping;
    List<Storage> registeredStubs;
        
    Skeleton<Service> serviceSkeleton;    
    Skeleton<Registration> registrationSkeleton;
    
    private static final int REPLICA_THRESHOLD = 2;
    
         
    /** Creates the naming server object.
        <p>
        The naming server is not started.
     */
    public NamingServer()
    {
    	fileTree = new FileTree();
    	
    	registeredStubs = new ArrayList<Storage>();
    	serverToClientStubMapping = new HashMap<Storage,Command>();
    	
    	InetSocketAddress serviceSocketAddress = new InetSocketAddress(NamingStubs.SERVICE_PORT);
    	serviceSkeleton = new Skeleton<Service>(Service.class, this, serviceSocketAddress);
    	
    	InetSocketAddress registratonSocketAddress = new InetSocketAddress(NamingStubs.REGISTRATION_PORT);
    	registrationSkeleton = new Skeleton<Registration>(Registration.class, this, registratonSocketAddress);
    }
 
    /** Starts the naming server.
        <p>
        After this method is called, it is possible to access the client and
        registration interfaces of the naming server remotely.

        @throws RMIException If either of the two skeletons, for the client or
                             registration server interfaces, could not be
                             started. The user should not attempt to start the
                             server again if an exception occurs.
     */
    public synchronized void start() throws RMIException
    {
    	serviceSkeleton.start();
    	registrationSkeleton.start();
    	System.out.println("Started the Naming Server");
    }

    /** Stops the naming server.
        <p>
        This method commands both the client and registration interface
        skeletons to stop. It attempts to interrupt as many of the threads that
        are executing naming server code as possible. After this method is
        called, the naming server is no longer accessible remotely. The naming
        server should not be restarted.
     */
    public void stop()
    {
    	serviceSkeleton.stop();
    	registrationSkeleton.stop();
    	stopped(null);
    }

    /** Indicates that the server has completely shut down.
        <p>
        This method should be overridden for error reporting and application
        exit purposes. The default implementation does nothing.

        @param cause The cause for the shutdown, or <code>null</code> if the
                     shutdown was by explicit user request.
     */
    protected void stopped(Throwable cause)
    {
        System.out.println("Stopped Naming Server");
    }

    // The following public methods are documented in Service.java.
    @Override
    public void lock(Path path, boolean exclusive) throws FileNotFoundException, InterruptedException
    {
    	System.out.println();
    //  Check that the path is valid
        if (path == null) {
            throw new NullPointerException();
        }
        
        FileNode lockNode = fileTree.getNode(path);     
        if(lockNode == null)
        {
            throw new FileNotFoundException("File to be locked is not found");
        }
        
        // Lock the file
        lockSynchronised(path, exclusive);
        if(exclusive)
    	{
        	System.out.println("Acquired exclusive lock on " + path);
    	}
    	else
    	{
        	System.out.println("Acquired shared lock on " + path);
    	}
        
        if (!exclusive &&lockNode.isFile) {
            // Increment the number of requests
            lockNode.numRequests++;
            
            // Replicate the file if the number of files is large (> REPLICA_THRESHOLD)
            if (lockNode.numRequests > REPLICA_THRESHOLD) {
            	System.out.println("Number of requests are " + lockNode.numRequests);
                lockNode.numRequests = 0;
                ReplicationCheck r = new ReplicationCheck(path);
                r.start();
            }
        } else if (lockNode.isFile){
            // Invalidate all but one copies
            if (lockNode.numReplicas == 1) {
                return;
            } else {
                // remove all but the first one
                
                // Change the number of replicas to reflect the change
                // NOTE : The Storage and Command lists are not changed so that restoration 
                // can happen on unlocking
                lockNode.numReplicas = 1;
                
                for (int i = 1; i < lockNode.getStorages().size(); i++) {
                    try {
                        lockNode.getCommands().get(i).delete(path);
                    } catch (RMIException e) {
                        e.printStackTrace();
                    }
                }       
            }
        }        
    } 
    
    public synchronized void lockSynchronised(Path path, boolean exclusive) throws FileNotFoundException, InterruptedException
    {
        ArrayList<FileNode> descendentTree = new ArrayList<FileNode>();     
        FileNode lockNode = fileTree.getNode(path);
        
        if(lockNode == null)
        {
            throw new FileNotFoundException("File to be locked is not found");
        }
        
        while(lockNode != null)
        {
            descendentTree.add(lockNode);
            lockNode = lockNode.getParent();
        }
        
        for(int i = descendentTree.size()-1; i > 0 ; i--)
        {
            while(descendentTree.get(i).writeLock || (
            descendentTree.get(i).threadIdQueue.size() != 0 && java.lang.Thread.currentThread().getId() != descendentTree.get(i).threadIdQueue.get(0).longValue()))
            {
                if(!descendentTree.get(i).threadIdQueue.contains(java.lang.Thread.currentThread().getId()))
                {
                    descendentTree.get(i).threadIdQueue.add(java.lang.Thread.currentThread().getId());
                }
                wait();
            }
            
            if(descendentTree.get(i).threadIdQueue.size() == 0)
            {
                descendentTree.get(i).readLockCounter++;
            }
            else if(descendentTree.get(i).threadIdQueue.size() != 0 && java.lang.Thread.currentThread().getId() == descendentTree.get(i).threadIdQueue.get(0).longValue())
            {
                descendentTree.get(i).threadIdQueue.remove(0);
                descendentTree.get(i).readLockCounter++;
            }

            //so that if there is another read thread right after this in the queue it can get the lock
            notifyAll(); 
        }
        
        if(descendentTree.size() != 0)
        {
            if(exclusive)
            {   
                while(descendentTree.get(0).readLockCounter > 0 || descendentTree.get(0).writeLock || (
                        descendentTree.get(0).threadIdQueue.size()!= 0 
                        && java.lang.Thread.currentThread().getId() != descendentTree.get(0).threadIdQueue.get(0).longValue()))
                {
                    if(!descendentTree.get(0).threadIdQueue.contains(java.lang.Thread.currentThread().getId()))
                    {
                        descendentTree.get(0).threadIdQueue.add(java.lang.Thread.currentThread().getId());
                    }
                    wait();
                }
                if(descendentTree.get(0).threadIdQueue.size() == 0)
                {
                    descendentTree.get(0).writeLock = true; 
                }
                else if(descendentTree.get(0).threadIdQueue.size()!= 0 
                        && java.lang.Thread.currentThread().getId() == descendentTree.get(0).threadIdQueue.get(0).longValue())
                {
                    descendentTree.get(0).threadIdQueue.remove(0);
                    descendentTree.get(0).writeLock = true; 
                }
            }
            else
            {
                while(descendentTree.get(0).writeLock ||
                        (descendentTree.get(0).threadIdQueue.size() != 0 
                        && java.lang.Thread.currentThread().getId() != descendentTree.get(0).threadIdQueue.get(0).longValue()))
                {
                    if(!descendentTree.get(0).threadIdQueue.contains(java.lang.Thread.currentThread().getId()))
                    {
                        descendentTree.get(0).threadIdQueue.add(java.lang.Thread.currentThread().getId());
                    }
                    wait();
                }

                if(descendentTree.get(0).threadIdQueue.size() == 0)
                {
                    descendentTree.get(0).readLockCounter++;
                }
                else if(descendentTree.get(0).threadIdQueue.size() != 0 
                        && java.lang.Thread.currentThread().getId() == descendentTree.get(0).threadIdQueue.get(0).longValue())
                {
                    descendentTree.get(0).threadIdQueue.remove(0);
                    descendentTree.get(0).readLockCounter++;
                }
            }
            notifyAll();
        }
    }
    
    @Override
    public void unlock(Path path, boolean exclusive) throws RMIException
    { 
        // Restore copies
        FileNode node = fileTree.getNode(path);
        
        if(node == null)
        {
            throw new IllegalArgumentException("The file to be unlocked does not exist");
        }
        
        if (node.numReplicas != node.getStorages().size()) {
            restoreCopies(path);
        }
        // Unlocked after restoring so nobody else changes the file while replication 
        unlockSynchronised(path, exclusive);
        
        if(exclusive)
    	{
        	System.out.println("Released exclusive lock on " + path);
    	}
    	else
    	{
        	System.out.println("Released shared lock on " + path);
    	}
    	System.out.println();
    }

    public synchronized void unlockSynchronised(Path path, boolean exclusive)
    {
        FileNode unlockNode = fileTree.getNode(path);
        
        boolean mainNodeDone = false;
        while(unlockNode != null)
        {
            if(!mainNodeDone)
            {
                if(exclusive)
                {
                    unlockNode.writeLock = false;               
                }
                else
                {
                    unlockNode.readLockCounter--;
                }
                mainNodeDone = true;
            }
            else
            {
                unlockNode.readLockCounter--;
            }
            notifyAll();
            unlockNode = unlockNode.getParent();
        }
    }
    
    private void restoreCopies(Path path) {
        FileNode node = fileTree.getNode(path);
        for (int i = 1; i < node.numReplicas; i++) {
            try {
            	int max = node.getStorages().size() - 1;
            	int min = 0;
            	int randomNum = ThreadLocalRandom.current().nextInt(min, max + 1);
            	// System.out.println("Getting random num:" + randomNum + " when size is: " + max + 1);
                node.getCommands().get(i).copy(path, node.getStorages().get(randomNum));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (RMIException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        node.numReplicas = node.getStorages().size();
    }

    @Override
    public boolean isDirectory(Path path) throws FileNotFoundException, InterruptedException, RMIException
    {
    	FileNode node = fileTree.getNode(path);
    	if(node == null)
        {
            throw new FileNotFoundException("Path is not a valid file/directory");
        }
        if(path.isRoot())
        {
            return true;
        }
//        lock(path.parent(), false);
        boolean result = !node.isFile;
//        unlock(path.parent(), false);

        return result;
    }

    @Override
    public String[] list(Path directory) throws FileNotFoundException, InterruptedException, RMIException
    {
        FileNode node = fileTree.getNode(directory);
        if(node == null || node.isFile)
        {
            throw new FileNotFoundException("Path is not a valid directory");
        }
//        lock(directory, false);
        String[] children = node.listChildren();
//        unlock(directory, false);
        System.out.println("Listing the directory:" + directory);
        return children;
    }
    
    
    /** Creates the given file, if it does not exist.

    @param file Path at which the file is to be created.
    @return <code>true</code> if the file is created successfully,
            <code>false</code> otherwise. The file is not created if a file
            or directory with the given name already exists.
    @throws FileNotFoundException If the parent directory does not exist.
    @throws IllegalStateException If no storage servers are connected to the
                                  naming server.
    @throws RMIException If the call cannot be completed due to a network
                         error.
     */

    @Override
    public boolean createFile(Path file) throws RMIException, FileNotFoundException, InterruptedException
    {
        if(file == null)
        {
            throw new NullPointerException("CreateFile does not take null as file argument");
        }
        if(file.isRoot())
        {
            return false;
        }
        if(registeredStubs.size() == 0)
        {
            throw new IllegalStateException("No servers registered");
        }
        if(fileTree.getNode(file.parent()) == null || fileTree.getNode(file.parent()).isFile)
        {
            throw new FileNotFoundException("Parent directory does not exist or is a file");
        }
        
//        lock(file.parent(), true);
        if(fileTree.getNode(file) != null)
        {
        	System.out.println("File " + file + " already exists!");
//            unlock(file.parent(), true);
            return false;
        }
        
        Storage serverStub = null;
        Command commandStub = null;
        FileNode node = fileTree.getNode(file.parent());
        List<Storage> storageServers = node.getStorages();
        
        int randomNum = 0;
        
        if(file.parent().isRoot() || storageServers == null)
        {
        	int max = registeredStubs.size() - 1;
        	int min = 0;
        	randomNum = ThreadLocalRandom.current().nextInt(min, max + 1);
//        	System.out.println("Min : " + min + " Max : " + max + " chosen :" + randomNum);
            serverStub = registeredStubs.get(randomNum);
//        	System.out.println("Server selected: " + serverStub.toString() + " i.e " + randomNum);
            commandStub = serverToClientStubMapping.get(serverStub);
            fileTree.addNode(file, serverStub, commandStub, false);
            commandStub.create(file);
        }
        else
        {
        	int max = storageServers.size() - 1;
        	int min = 0;
        	randomNum = ThreadLocalRandom.current().nextInt(min, max + 1);
//        	System.out.println("Min : " + min + " Max : " + max + " chosen :" + randomNum);
        	serverStub = node.getStorages().get(randomNum);
        	commandStub = node.getCommands().get(randomNum);
            fileTree.addNode(file, serverStub, commandStub, false);
            node.getCommands().get(randomNum).create(file);
        }
        
//        unlock(file.parent(), true);
        System.out.println("Creating the file :" + file + " on storage server:" + serverStub.toString());
        return true;
    }
    
    /** Creates the given directory, if it does not exist.

        @param directory Path at which the directory is to be created.
        @return <code>true</code> if the directory is created successfully,
                <code>false</code> otherwise. The directory is not created if
                a file or directory with the given name already exists.
        @throws FileNotFoundException If the parent directory does not exist.
        @throws RMIException If the call cannot be completed due to a network
                             error.
     */
    @Override
    public boolean createDirectory(Path directory) throws RMIException, FileNotFoundException, InterruptedException
    {
        if(serverToClientStubMapping.size() == 0)
        {
            throw new IllegalStateException("No servers registered");
        }
        if(directory == null)
        {
            throw new NullPointerException("CreateDirectory does not take a null argument");
        }
        
        if(directory.isRoot())
        {
            return false;
        }
        
        if(fileTree.getNode(directory.parent()) == null || fileTree.getNode(directory.parent()).isFile)
        {
            throw new FileNotFoundException("Parent directory does not exist or is a file");
        }
        
//        lock(directory.parent(), true);
        
        if (fileTree.getNode(directory) != null) 
        {
        	System.out.println("Directory " + directory + " already exists!");
//            unlock(directory.parent(), true);
            return false;
        } 
        else
        {
            
            Storage serverStub = null;
            Command commandStub = null;
            FileNode parent = fileTree.getNode(directory.parent());
            List<Storage> storageServers = parent.getStorages();
          
            int randomNum = 0;
            if(directory.parent().isRoot() || storageServers == null)
            {
            	int max = registeredStubs.size() - 1;
            	int min = 0;
            	randomNum = ThreadLocalRandom.current().nextInt(min, max + 1);
//            	System.out.println("Min : " + min + " Max : " + max + " chosen :" + randomNum);
                serverStub = registeredStubs.get(randomNum);
                commandStub = serverToClientStubMapping.get(serverStub);
                fileTree.addNode(directory, serverStub, commandStub, true);
            }
            else 
            {
            	int max = storageServers.size() - 1;
            	int min = 0;
            	randomNum = ThreadLocalRandom.current().nextInt(min, max + 1);
//            	System.out.println("Min : " + min + " Max : " + max + " chosen :" + randomNum);
                fileTree.addNode(directory, storageServers.get(randomNum), serverToClientStubMapping.get(storageServers.get(randomNum)), true);
            }    
            System.out.println("Created a virtual directory on storage server: " + serverStub.toString());
        }
//        unlock(directory.parent(), true);
        return true;
    }

    @Override
    public boolean delete(Path path) throws FileNotFoundException, RMIException, InterruptedException
    {       
        if(path.isRoot())
        {
            return false;
        }
        FileNode node = fileTree.getNode(path);
        if(node == null)
        {
        	System.out.println("File " + path + " does not exist!");
            throw new FileNotFoundException("File/Directory does not exist");
        }
        else
        {
//            lock(path.parent(), true);
            fileTree.deleteNode(path);
//            unlock(path.parent(), true);
        }
        System.out.println("Deleted the file: " + path);
        return true;
    }

    @Override
    public Storage getStorage(Path file) throws FileNotFoundException
    {
        FileNode node = fileTree.getNode(file);
        
        if(node != null && node.isFile)
        {           
            return node.getStorage();
        }
        if(node != null && !node.isFile)
        {
            throw new FileNotFoundException("Get Storage takes only files");            
        }
        throw new FileNotFoundException("Requested file not present in the file system");
    }

    // The method register is documented in Registration.java.
    @Override
    public Path[] register(Storage client_stub, Command command_stub,
                           Path[] files)
    {
        /*
        Registration requires the naming server to lock the root directory for
        exclusive access. Therefore, it is best done when there is not heavy
        usage of the file system.

        @param client_stub Storage server client service stub. This will be
                           given to clients when operations need to be performed
                           on a file on the storage server.
        @param command_stub Storage server command service stub. This will be
                            used by the naming server to issue commands that
                            modify the directory tree on the storage server.
        @param files The list of files stored on the storage server. This list
                     is merged with the directory tree already present on the
                     naming server. Duplicate filenames are dropped.
        @return A list of duplicate files to delete on the local storage of the
                registering storage server.
        @throws IllegalStateException If the storage server is already
                                      registered.
        @throws NullPointerException If any of the arguments is
                                     <code>null</code>.
        @throws RMIException If the call cannot be completed due to a network
                             error.
        */
        if(files == null || client_stub == null || command_stub == null)
        {
            throw new NullPointerException("One or more arguments are null");
        }
        if(registeredStubs.contains(client_stub))
        {
            throw new IllegalStateException("Server has already been registered");
        }
        List<Path> filesToBeDeleted = new ArrayList<Path>();
        for(Path file : files)
        {
            if(!file.isRoot() && fileTree.getNode(file) != null)
            {
                filesToBeDeleted.add(file);
            }
            else
            {
                fileTree.addNode(file, client_stub, command_stub, false);
            }
        }
        System.out.println("Storage Server registered : " + client_stub.toString());
        registeredStubs.add(client_stub);
        serverToClientStubMapping.put(client_stub, command_stub);
        System.out.println("Registered servers : " + registeredStubs.size());
        return filesToBeDeleted.toArray(new Path[filesToBeDeleted.size()]);
    }

    // A thread of this class is spawned if a file has to be replicated
    public class ReplicationCheck extends Thread {
        Path path;
        
        public ReplicationCheck(Path path) {
            this.path = path;
        }
        
        public void run() {
            try {
                lock(path, false);
            } catch (FileNotFoundException | InterruptedException e1) {
                e1.printStackTrace();
            }
                
            createNewReplica(path);
                
            try {
                unlock(path, false);
            } catch (RMIException e) {
                e.printStackTrace();
            }
        }
        
        // Creates a new replica
        private void createNewReplica(Path path) {            
            FileNode node = fileTree.getNode(path);
//            System.out.println("number of storage severs that / exists is " + node.getStorages().size());
            if (registeredStubs.size() <= node.getStorages().size()) {
                System.out.println("Failed to create Replica because all servers already have a copy");
                return;
            } else {
                // Choose a storage that does not contain the file
                Storage storage = getSpareStorage(node);
                Storage existingStorage = node.getStorage();
                Command command = serverToClientStubMapping.get(storage);
                
                try {
//                	System.out.println("File being copied: "+ path);
                    command.copy(path, existingStorage);
                    node.addServers(storage, command);
                    node.numReplicas++;
                } catch (RMIException | IOException e) {
                    e.printStackTrace();
                }
            }   
        }
        
        private Storage getSpareStorage(FileNode node) {
            List<Storage> existingServers = node.getStorages();
            for (Storage storage : registeredStubs) {
                if (!existingServers.contains(storage)) {
                    return storage;
                }
            }
            return null; // Will never reach here
        }
    }
}