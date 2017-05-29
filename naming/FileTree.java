package naming;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import common.Path;
import rmi.RMIException;
import storage.Command;
import storage.Storage;

public class FileTree {
	
	private FileNode root;

	FileTree()
	{
		root = new FileNode(null, null, false, null, "root");
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
//		   System.out.println(children.get(i).getPath());
		   DFS(children.get(i));
//		   System.out.println();
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
		FileNode foundNode = null;
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
			foundNode = node;
		}
		return foundNode;
	}
	
	public List<FileNode> getDescendents()
	{
	   List<FileNode> descendents = new ArrayList<FileNode>();
//	   getDescendents(new ArrayList<FileNode>(children.values()), descendents);
	   return descendents;
    }
   
   public void deleteChildrenRecursively(List<FileNode> nodeChildren) throws RMIException
   {	   
	   for(FileNode node : nodeChildren)
	   {
		   List<FileNode> nextChildren = node.getChildren();
		   if(nextChildren != null)
		   {
			   deleteChildrenRecursively(nextChildren);
		   }
		   for(Command command: node.getCommands())
		   {
			   command.delete(node.getPath());
			   node.deleteCommand(command);
		   }
		   node.getParent().deleteChild(node);
	   }
   }

	public void deleteNode(Path path) throws RMIException
	{
		FileNode node = getNode(path);
		if(!node.isFile && node.getChildren().size() != 0)
		{
			deleteChildrenRecursively(node.getChildren());
		}
		node.getParent().deleteChild(node);		
	}

	public void addNode(Path path, Storage clientStub, Command commandStub, boolean isDirectory)
	{
		if(clientStub == null)
		{
			System.out.println("Yo" + path);
		}
//		System.out.println("Adding node:" + path.toString());
		if(path == null)
		{
			return;
		}
		FileNode node = root;
		Iterator<String> pathIterator = path.iterator();
		
		while(pathIterator.hasNext())
		{
			String pathComponent = pathIterator.next();
			boolean isFile = false;
			if(node.getChild(pathComponent) == null)
			{
				FileNode newNode = null;
				if(!pathIterator.hasNext() && !isDirectory)				//is the last component i.e the file component
				{
					isFile = true;
				}
				System.out.println("Adding: " + clientStub + ":" + commandStub);
				newNode = new FileNode(clientStub, commandStub, isFile, path, pathComponent);	//create the new node
				node.addChild(pathComponent, newNode);						//add it to the parents childMap
				newNode.setParent(node);
				node = newNode;												//get reference to it
			}
			else
			{
				node = node.getChild(pathComponent);						//else just get reference to it
			}
		}
		
		System.out.println("Servers in list:" + getNode(path).getServers().size());
		for(Storage server: getNode(path).getServers())
		{
			System.out.println(server);
		}
	}
}
