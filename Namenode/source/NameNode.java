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

import Datanode.source.DataNode;
import Proto.Hdfs;
	
public class NameNode implements INameNode {
	
	public byte[] openFile(byte[] inp) throws RemoteException
	{
		return null;
	}

	public byte[] closeFile(byte[] inp) throws RemoteException
	{
		return null;
	}

	public byte[] getBlockLocations(byte[] inp ) throws RemoteException
	{
		return null;
	}

	public byte[] assignBlock(byte[] inp ) throws RemoteException
	{
		return null;
	}

	public byte[] list(byte[] inp ) throws RemoteException
	{
		return null;
	}

	public byte[] blockReport(byte[] inp ) throws RemoteException
	{
		return null;
	}

	public byte[] heartBeat(byte[] inp ) throws RemoteException
	{
		try
		{
			Hdfs.HeartBeatRequest req = Hdfs.HeartBeatRequest.parseFrom(inp);
			int id = req.getId();
			System.err.println("HeartBeat received from DN : " + String.valueOf(id));

			Hdfs.HeartBeatResponse.Builder hbr_builder = Hdfs.HeartBeatResponse.newBuilder().setStatus(1);
			return hbr_builder.build().toByteArray();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return null;
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
