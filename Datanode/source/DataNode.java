package Datanode.source;

import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import java.lang.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
	
public class DataNode implements IDataNode {
	
	public byte[] readBlock(byte[] inp) throws RemoteException
	{
		byte[] a;
		a = new byte[10];
		return a;
	}

	public byte[] writeBlock(byte[] inp) throws RemoteException
	{
		byte[] a;
		a = new byte[10];
		return a;
	}

	public static void main(String args[])
	{
		try
		{
			DataNode obj = new DataNode();
			IDataNode stub = (IDataNode) UnicastRemoteObject.exportObject(obj, 0);
			
			Registry registry = LocateRegistry.getRegistry();
			registry.bind("DataNode", stub);
		}
		catch (Exception e)
		{
			System.err.println("Server exception: " + e.toString());
			e.printStackTrace();
		}
	}
}
