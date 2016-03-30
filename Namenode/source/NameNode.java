package Namenode.source;

import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import java.lang.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
	
public class NameNode implements INameNode {
	
	public byte[] openFile(byte[] inp) throws RemoteException
	{
		byte[] a;
		a = new byte[10];
		return a;
	}

	public byte[] closeFile(byte[] inp) throws RemoteException
	{
		byte[] a;
		a = new byte[10];
		return a;
	}

	public byte[] getBlockLocations(byte[] inp ) throws RemoteException
	{
		byte[] a;
		a = new byte[10];
		return a;
	}

	public byte[] assignBlock(byte[] inp ) throws RemoteException
	{
		byte[] a;
		a = new byte[10];
		return a;
	}

	public byte[] list(byte[] inp ) throws RemoteException
	{
		byte[] a;
		a = new byte[10];
		return a;
	}

	public byte[] blockReport(byte[] inp ) throws RemoteException
	{
		byte[] a;
		a = new byte[10];
		return a;
	}

	public byte[] heartBeat(byte[] inp ) throws RemoteException
	{
		byte[] a;
		a = new byte[10];
		return a;
	}

	public static void main(String args[])
	{
		try
		{
			NameNode obj = new NameNode();
			INameNode stub = (INameNode) UnicastRemoteObject.exportObject(obj, 0);
			
			Registry registry = LocateRegistry.getRegistry();
			registry.bind("NameNode", stub);
		}
		catch (Exception e)
		{
			System.err.println("Server exception: " + e.toString());
			e.printStackTrace();
		}
	}
}
