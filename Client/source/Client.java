package Client.source;

import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import java.lang.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import Namenode.source.NameNode;
import Namenode.source.INameNode;
import Datanode.source.DataNode;
import Datanode.source.IDataNode;

public class Client
{
	public static void main(String args[])
	{
		try
		{
			Registry registry = LocateRegistry.getRegistry();
			INameNode stub=(INameNode)registry.lookup("NameNode");  
		}
		catch (Exception e)
		{

		}
		while(true)
		{

		}
	}
}