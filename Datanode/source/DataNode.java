package Datanode.source;

import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import java.lang.*;
import java.util.*;
import java.io.*;

import Namenode.source.*;
import Proto.Hdfs;
	
public class DataNode implements IDataNode {

	public byte[] readBlock(byte[] inp) throws RemoteException
	{
		return null;
	}

	public byte[] writeBlock(byte[] inp) throws RemoteException
	{
		return null;
	}

	static class HeartBeat extends Thread
	{
		public HeartBeat(){}

		public void run()
		{
			try
			{
				while(true){
					Hdfs.HeartBeatRequest.Builder heartBeatRequestBuilder = Hdfs.HeartBeatRequest.newBuilder().setId(1);
					Registry registry = LocateRegistry.getRegistry();
					INameNode stub = (INameNode) registry.lookup("NameNode");
					byte[] response = stub.heartBeat(heartBeatRequestBuilder.build().toByteArray());

					Hdfs.HeartBeatResponse res = Hdfs.HeartBeatResponse.parseFrom(response);
					System.err.println("Heart Beat Response from NameNode " + String.valueOf(res.getStatus()));
					Thread.sleep(10000);
				}
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}

	public static void main(String args[])
	{
		try
		{
			DataNode obj = new DataNode();
			IDataNode stub = (IDataNode) UnicastRemoteObject.exportObject(obj, 0);
			
			Registry registry = LocateRegistry.getRegistry();
			registry.bind("DataNode", stub);

			HeartBeat heartBeat = new HeartBeat();
			heartBeat.start();
		}
		catch (Exception e)
		{
			System.err.println("Server exception: " + e.toString());
			e.printStackTrace();
		}
	}
}
