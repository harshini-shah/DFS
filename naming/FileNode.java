package naming;

import java.io.*;
import java.net.*;
import java.util.*;

import rmi.*;
import common.*;
import storage.*;


public class FileNode
{
   List<Storage> serverList;
   List<Command> commandList;
   Map<String, FileNode> children;
   boolean isFile;
   String pathComponent;
   FileNode parent;
   Path path;
   
   FileNode(Storage client_stub, Command command_stub, boolean isFile, Path path, String pathComponent)
   {
	   this.path = path;
	   this.pathComponent = pathComponent;
	   serverList = new ArrayList<Storage>();
	   serverList.add(client_stub);
	   
	   commandList = new ArrayList<Command>();
	   commandList.add(command_stub);
	   
	   this.isFile = isFile;
	   if(isFile)
	   {
		   children = null;
	   }
	   else 
	   {
		   children = new HashMap<String, FileNode>();
	   }
   } 
   
   public String getPathComponentName()
   {
	   return pathComponent;
   }
   
   public List<Command> getCommands()
   {
	   return commandList;
   }
   
   public List<FileNode> getChildren()
   {
	   if(children != null)
	   {
		   return new ArrayList<FileNode>(children.values());		   
	   }
	   return null;
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
   
   public void setParent(FileNode givenParent)
   {
	   parent =  givenParent;
   }
   
   public void deleteChild(FileNode child)
   {
	   children.remove(child);
   }
   
   public FileNode getParent()
   {
	   return parent;
   }
   
   public Path getPath()
   {
	   return path;
   }
   
   public void deleteCommand(Command command)
   {
	   commandList.remove(0);
	   serverList.remove(0);
   }
   
//   private void deleteServer(Storage server)
//   {
//	   serverList.remove(0);
//   }
//   
   public List<Storage> getServers()
   {
	   return serverList;
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
		   if(nodeChildren != null)
		   {
			   getDescendents(new ArrayList<FileNode>(node.children.values()), descendents);
		   }
	   } 
   }
   
   
   public Storage getStorage()
   {
	   if(!serverList.isEmpty())
	   {
		   return serverList.get(0);
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
   
}
