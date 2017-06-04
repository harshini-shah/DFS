/*
 * The synchronized common Distributed File System Directory Tree.
 */

package naming;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import common.Path;
import rmi.RMIException;
import storage.Command;
import storage.Storage;

public class FileTree {
	
	private FileNode root;

	FileTree()
	{
		root = new FileNode(null, null, false, new Path(), "root");
	}
	
	public FileNode getRoot()
	{
		return root;
	}
	
	private void DFS(FileNode child)
	{
		System.out.print(child.getPathComponentName()+"/");
		if(child.getChildren() == null)
		{
		   System.out.println();
		   return;
		}
		for(int i = 0; i < child.getChildren().size(); i++)
		{
			DFS(child.getChildren().get(i));
		}
	}
	
	public void printDirectories()
    {
	   FileNode node = root;
	   List<FileNode> children = node.getChildren();
	   if(children == null)
	   {
		   return;
	   }
	   for(int i = 0; i < children.size(); i++)
	   {
		   DFS(children.get(i));
	   }	   
    }
	   	
	public FileNode getNode(Path path)
	{
	    FileNode node = root;
		if(path.isRoot())
		{
			return root;
		}
		
		Iterator<String> pathIterator = path.iterator();
		while(pathIterator.hasNext())
		{
			String pathComponent = pathIterator.next();
			node = node.getChild(pathComponent);
			if(node == null)
			{
				return node;
			}
			if(node.isFile)
			{
				return node;
			}
		}
	    return node;
	}
	
	public List<FileNode> getDescendents()
	{
	    List<FileNode> descendents = new ArrayList<FileNode>();
	    descendents = root.getDescendents();
	    return descendents;
    }
   
	public void deleteNode(Path path) throws RMIException
	{      
	    FileNode node = getNode(path);
	    for (Command command : node.getCommands()) {
	        command.delete(path);    
	    }
	    
	    node.getParent().deleteChild(node);             
	}

	public void addNode(Path path, Storage clientStub, Command commandStub, boolean isDirectory)
	{
		if(path == null)
		{
		    throw new NullPointerException("Path to be added is null");
		}
		
		FileNode node = root;
		Iterator<String> pathIterator = path.iterator();
		
		while(pathIterator.hasNext())
		{
			String pathComponent = pathIterator.next();
			boolean isFile = false;
			if(node.getChild(pathComponent) == null)
			{
			    // Subdirectory does not exist
				FileNode newNode = null;
				if(!pathIterator.hasNext() && !isDirectory)
				{
				    //is the last component i.e the file component
					isFile = true;
				}
				
				newNode = new FileNode(clientStub, commandStub, isFile, path, pathComponent);   //create the new node
				node.addChild(pathComponent, newNode);  //add it to the parents childMap
                newNode.setParent(node);
                node = newNode;     //get reference to it														
			} else {
			    // Subdirectory exists
			    node = node.getChild(pathComponent);
			    // Add the Storage and Command servers if not already there
			    if (!node.getStorages().contains(clientStub)) {
		             node.addServers(clientStub, commandStub);
			    }
			}
		}
	}
}