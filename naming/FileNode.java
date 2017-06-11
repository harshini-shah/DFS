/*
 * Each file/directory in the File System is of type FileNode. 
 * 
 * TODO : Radomize the storage server returned if there are multiple copies. 
 * Method to be modified is: getStorage().
 */

package naming;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import common.Path;
import storage.Command;
import storage.Storage;

public class FileNode
{
    private List<Storage> storageList;
    private List<Command> commandList;
    private Map<String, FileNode> children;
    boolean isFile;
    private String pathComponent;    // Actual name of this Node (can be file/dir)
    private FileNode parent;
    private Path path;
   
    int readLockCounter;
    boolean writeLock;
    int writeRequests;
    List<Long> threadIdQueue;
    
    private int storageNum;

    int numRequests;
    int numReplicas;
   
    FileNode(Storage client_stub, Command command_stub, boolean isFile, Path path, String pathComponent)
    {
//    	System.out.println("Adding node with stubs:" + command_stub + " and " + client_stub);
        this.path = path;
        this.pathComponent = pathComponent;
        storageList = new ArrayList<Storage>();
        storageList.add(client_stub);
        
        commandList = new ArrayList<Command>();
        commandList.add(command_stub);
        
        threadIdQueue = new ArrayList<Long>();
    	   
        this.numRequests = 0;
        this.numReplicas = 1;
        this.readLockCounter = 0;
        this.writeRequests = 0;
        this.writeLock = false;
        this.parent = null;
        
        this.storageNum = 0;
    	   
        this.isFile = isFile;
        
        if(isFile) {
            children = null;
        } else {
            children = new HashMap<String, FileNode>();
        }
   } 
   
   public String getPathComponentName()
   {
	   return pathComponent;
   }
   
   public Path getPath()
   {
	   return path;
   }

   public List<Command> getCommands()
   {
	   return commandList;
   }
   
   public List<Storage> getStorages()
   {
       return storageList;
   }
   
   public void deleteServers(Storage storage, Command command)
   {
	   commandList.remove(command);
	   storageList.remove(storage);
   }
   
   public void addServers(Storage storage, Command command) {
       this.commandList.add(command);
       this.storageList.add(storage);
       return;
   }
   
   public void setParent(FileNode givenParent)
   {
	   parent =  givenParent;
   }

   public FileNode getParent()
   {
	   return parent;
   }

   public String[] listChildren()
   {
	   String[] childrenNames = new String[children.size()];
	   
	   int i = 0;
	   for(String childName: children.keySet())
	   {
		   childrenNames[i] = childName;
		   i++;
	   }
	   
	   return childrenNames;
   }
   
   public List<FileNode> getChildren()
   {
	   if(children != null)
	   {
		   return new ArrayList<FileNode>(children.values());		   
	   }
	   return null;
   }

   public void addChild(String pathComponent, FileNode childNode)
   {
	   children.put(pathComponent, childNode);
   }

   public FileNode getChild(String pathComponent)
   {
	   if(children.containsKey(pathComponent))
	   {
		   return children.get(pathComponent);
	   }
	   
	   return null;
   }

   private void printChildren()
   {
	   int i = 0;
	   for(String child : listChildren())
	   {
		   System.out.println(i + ":" + child);
		   i++;
	   } 
   }

   public void deleteChild(FileNode child)
   {
       children.remove(child.pathComponent);
   }
   
   public List<FileNode> getDescendents()
   {
	   List<FileNode> descendents = new ArrayList<FileNode>();
	   getDescendents(new ArrayList<FileNode>(children.values()), descendents);
	   return descendents;
   }
   
   public void getDescendents(List<FileNode> nodeChildren, List<FileNode> descendents)
   {	   
	   for(FileNode node : nodeChildren)
	   {
		   descendents.add(node);
		   if(node.children != null)
		   {
			   getDescendents(new ArrayList<FileNode>(node.children.values()), descendents);
		   }
	   } 
   }
   
   // Get the storage server associated with this file node
   public Storage getStorage()
   {
       // Check number of replicas - if this is n, then only the first n servers in both the
       // lists are valid ones
	   if(!storageList.isEmpty())
	   {
		   int max = storageList.size() - 1;
		   int min = 0;
		   int randomNum = ThreadLocalRandom.current().nextInt(min, max + 1);
//       	   System.out.println("Min : " + min + " Max : " + max + " chosen :" + randomNum);
//		   int randomNum = storageNum++ % storageList.size();
		   return storageList.get(randomNum);
	   }
	   return null;
   }
   
}