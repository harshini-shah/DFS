package naming;

import java.io.*;
import java.net.*;
import java.util.*;

import rmi.*;
import common.*;
import storage.*;

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
    
    // Added for replication - Maintaing num of requesters for each path
    
    private static final float ALPHA = 1.0F;
    private static final int REPLICA_UPPER_BOUND = 5;
    
    
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
    //  Check that the path is valid
        if (path == null) {
            throw new NullPointerException();
        }
        
        FileNode lockNode = fileTree.getNode(path);
        
        if(lockNode == null)
        {
            throw new FileNotFoundException("File to be locked is not found");
        }
        
        lockSynchronised(lockNode, exclusive);
        
        if (!exclusive) {
            lockNode.numRequests++;
        }
        
//        try {
//            checkForReplication(lockNode, path);
//        } catch (RMIException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
        
        new ReplicationCheck(path).start();
        
    }
    
    public class ReplicationCheck extends Thread {
        
        Path path;
        
        public ReplicationCheck(Path path) {
            this.path = path;
        }
        
        public void run() {
//            System.out.println("Running Replication Check thread");
            FileNode lockNode = fileTree.getNode(path);
          
            System.out.println("COUNT IS " + lockNode.numRequests + " Servers is " + lockNode.serverList.size());
            if (lockNode.numRequests % 20 == 1) {
                System.out.println("Modulo is 1");
                if (lockNode.serverList.size() < ((lockNode.numRequests / 20) + 1)) {
                    System.out.println("Creating replica");
                    try {
                        lock(path, false);
                    } catch (FileNotFoundException e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    } catch (InterruptedException e1) {
                        // TODO Auto-generated catch block
                        e1.printStackTrace();
                    }

                    createNewReplica(path);
                    try {
                        unlock(path, false);
                    } catch (RMIException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }

                }
            }
            
        }
        
        // Creates a new replica
        private void createNewReplica(Path path) {
            
            System.out.println("************ CREATING REPLICA************8");
            FileNode node = fileTree.getNode(path);
            // TODO Auto-generated method stub
            if (registeredStubs.size() <= node.serverList.size()) {
                System.out.println("All storage servers already have a copy of the file");
            }
            
            // Choose a storage that does not contain the file
            Storage storage = getSpareStorage(node);
            System.out.println("Spare storage server is " + storage);
            Storage existingStorage = node.getStorage();
            
            try {
                serverToClientStubMapping.get(storage).copy(path, existingStorage);
                System.out.println("Called copy");
                node.serverList.add(storage);
            } catch (RMIException | IOException e) {
                e.printStackTrace();
            }
        }
        
        private Storage getSpareStorage(FileNode node) {
            List<Storage> existingServers = node.getServers();
            for (Storage storage : registeredStubs) {
                if (!existingServers.contains(storage)) {
                    return storage;
                }
            }
            
            return null; // Will never reach here
        }
    }
    
    private void createNewReplica(Path path) {
        
        System.out.println("************ CREATING REPLICA************8");
        FileNode node = fileTree.getNode(path);
        // TODO Auto-generated method stub
        if (registeredStubs.size() <= node.serverList.size()) {
            System.out.println("All storage servers already have a copy of the file");
        }
        
        // Choose a storage that does not contain the file
        Storage storage = getSpareStorage(node);
        System.out.println("Spare storage server is " + storage);
        Storage existingStorage = node.getStorage();
        
        try {
            serverToClientStubMapping.get(storage).copy(path, existingStorage);
            System.out.println("Called copy");
            node.serverList.add(storage);
        } catch (RMIException | IOException e) {
            e.printStackTrace();
        }
    }
    
    private Storage getSpareStorage(FileNode node) {
        List<Storage> existingServers = node.getServers();
        for (Storage storage : registeredStubs) {
            if (!existingServers.contains(storage)) {
                return storage;
            }
        }
        
        return null; // Will never reach here
    }
    
    public void checkForReplication(FileNode lockNode, Path path) throws FileNotFoundException, InterruptedException, RMIException {
        lockNode.numRequests++;
        System.out.println("COUNT IS " + lockNode.numRequests + " Servers is " + lockNode.serverList.size());
        if (lockNode.numRequests % 20 == 1) {
            System.out.println("Modulo is 1");
            if (lockNode.serverList.size() < ((lockNode.numRequests / 20) + 1)) {
                System.out.println("Creating replica");
                createNewReplica(path);
            }
        }
        
    }
    
    public synchronized void lockSynchronised(FileNode lockNode, boolean exclusive) throws FileNotFoundException, InterruptedException
    {
        ArrayList<FileNode> descendentTree = new ArrayList<FileNode>();     
        
        // All the parents have to be locked too
        while(lockNode != null)
        {
            descendentTree.add(lockNode);
            lockNode = lockNode.getParent();
        }
        
        for(int i = descendentTree.size()-1; i > 0 ; i--)
        {
            while(descendentTree.get(i).writeLock || descendentTree.get(i).writeRequests > 0)
            {
                wait();
            }            
            descendentTree.get(i).readLockCounter++;
        }
        
        if(descendentTree.size() != 0)
        {
            while(descendentTree.get(0).writeRequests > 0 || descendentTree.get(0).writeLock)
            {
                wait();
            }
            
            if(exclusive)
            {
                descendentTree.get(0).writeRequests++;
                while(descendentTree.get(0).readLockCounter > 0 || descendentTree.get(0).writeLock)
                {
                    wait();
                }
                descendentTree.get(0).writeRequests--;
                descendentTree.get(0).writeLock = true; 
//              System.out.println("Write lock on:" + descendentTree.get(0).path.toString() + " by thread : " + java.lang.Thread.currentThread().getId());  
            }
            else
            {
                
                if(descendentTree.get(0).writeRequests > 0 || descendentTree.get(0).writeLock)
                {
                    wait();
                }
                descendentTree.get(0).readLockCounter++;
//              System.out.println(s"Read lock on:" + descendentTree.get(0).path.toString() + " by thread : " + java.lang.Thread.currentThread().getId());          
            }
        }
    }
    
   

    public synchronized void unlockSynchronised(Path path, boolean exclusive)
    {
        FileNode unlockNode = fileTree.getNode(path);
        if(unlockNode == null)
        {
            throw new IllegalArgumentException("The file to be unlocked does not exist");
        }
        
        boolean mainNodeDone = false;
        while(unlockNode != null)
        {
            if(!mainNodeDone)
            {
                if(exclusive)
                {
//                  System.out.println("Write unlock on: " + unlockNode.path.toString() + " by thread : " + java.lang.Thread.currentThread().getId());
                    unlockNode.writeLock = false;               
                }
                else
                {
//                  System.out.println("Read unlock on: " + unlockNode.path.toString() + " by thread : " + java.lang.Thread.currentThread().getId());
                    unlockNode.readLockCounter--;
                }
                mainNodeDone = true;
            }
            else
            {
//              System.out.println("Read unlock on:" + unlockNode.path.toString() + " by thread : " + java.lang.Thread.currentThread().getId());
                unlockNode.readLockCounter--;
            }
            notifyAll();
            unlockNode = unlockNode.getParent();
        }
    }
    
    @Override
    public void unlock(Path path, boolean exclusive) throws RMIException
    {
        unlockSynchronised(path, exclusive);
        
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
        lock(path.parent(), false);
        boolean result = !node.isFile;
        unlock(path.parent(), false);

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
        System.out.println("Read lock acquired in list method by thread : " + java.lang.Thread.currentThread().getId());
        lock(directory, false);
        String[] children = node.listChildren();
        unlock(directory, false);
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
//      fileTree.printDirectories();
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
        
        System.out.println("Write lock acquired in createFile by thread : " + java.lang.Thread.currentThread().getId());
        lock(file.parent(), true);
        if(fileTree.getNode(file) != null)
        {
            unlock(file.parent(), true);
            return false;
        }
        
        Storage serverStub = null;
        Command commandStub = null;
        FileNode node = fileTree.getNode(file.parent());
        List<Storage> storageServers = node.getServers();
        
        if(file.parent().isRoot() || storageServers == null)
        {
            serverStub = registeredStubs.get(0);
            commandStub = serverToClientStubMapping.get(serverStub);
            fileTree.addNode(file, serverStub, commandStub, false);
            commandStub.create(file);
        }
        else
        {
            int i = 0;
            for(Storage storageServer: storageServers)
            {
                commandStub = node.getCommands().get(i);
                fileTree.addNode(file, storageServer, commandStub, false);
                commandStub.create(file);
                i++;
            }
        }
        
        
        unlock(file.parent(), true);
        
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
//      Check about this
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
        

        System.out.println("Write lock acquired in createDirectory by thread : " + java.lang.Thread.currentThread().getId());
        lock(directory.parent(), true);
        if(fileTree.getNode(directory) != null)
        {
            unlock(directory.parent(), true);
            return false;
        }
        else
        {
            Storage serverStub = null;
            Command commandStub = null;
            FileNode node = fileTree.getNode(directory.parent());
            List<Storage> storageServers = node.getServers();
            
            if(directory.parent().isRoot() || storageServers == null)
            {
                serverStub = registeredStubs.get(0);
                commandStub = serverToClientStubMapping.get(serverStub);
                fileTree.addNode(directory, serverStub, commandStub, true);
            }
            else
            {
                int i = 0;
                for(Storage storageServer: storageServers)
                {
                    commandStub = node.getCommands().get(i);
                    fileTree.addNode(directory, storageServer, commandStub, true);
//                  commandStub.create(directory);
                    i++;
                }
            }
            unlock(directory.parent(), true);
            return true;
        }       
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
            throw new FileNotFoundException("File/Directory does not exist");
        }
        else
        {
            System.out.println("Write lock acquired in delete by thread : " + java.lang.Thread.currentThread().getId());
            lock(path.parent(), true);
            fileTree.deleteNode(path);
            unlock(path.parent(), true);
        }
        return false;
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
        Registration requries the naming server to lock the root directory for
        exclusive access. Therefore, it is best done when there is not heavy
        usage of the filesystem.

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
//        System.out.println("client stub:" + client_stub);
//        System.out.println("Server stub:" + command_stub);
        registeredStubs.add(client_stub);
        serverToClientStubMapping.put(client_stub, command_stub);
//        System.out.println("Registering!");
//        fileTree.printDirectories();
        return filesToBeDeleted.toArray(new Path[filesToBeDeleted.size()]);

    }
}