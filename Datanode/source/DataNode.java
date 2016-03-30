package Datanode.source;

import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import java.lang.*;
import java.util.*;
import java.io.*;

import Namenode.source.INameNode;
import Proto.Hdfs;
import com.google.protobuf.ByteString;
	
public class DataNode implements IDataNode {

	private static String NameNode_IP = "54.174.209.93";
	private int ID;
	private static final int block_size = 33554432;

	public DataNode(int id)
	{
		ID = id;
	}

	public byte[] readBlock(byte[] inp) throws RemoteException
	{
		return null;
	}

	public byte[] writeBlock(byte[] inp) throws RemoteException
	{
		try
		{
			File dir = new File("Blocks");
			Hdfs.WriteBlockRequest writeBlockRequest = Hdfs.WriteBlockRequest.parseFrom(inp);

			int blockNum = writeBlockRequest.getBlockInfo().getBlockNumber();
			File blockFile = new File(dir, String.valueOf(blockNum));
			FileOutputStream fos = new FileOutputStream(blockFile);

			List<ByteString> dataString = writeBlockRequest.getDataList();
			for(ByteString byteString : dataString)
				fos.write(byteString.toByteArray());

			fos.close();

			File report = new File("BlockReport.txt");
			FileWriter fw = new FileWriter(report.getName(), true);

			BufferedWriter bw = new BufferedWriter(fw);

			bw.write(Integer.toString(blockNum));
			bw.newLine();
			bw.close();

			return Hdfs.WriteBlockResponse.newBuilder().setStatus(1).build().toByteArray();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return null;
	}

	static class HeartBeat extends Thread
	{
		private int ID;
		public HeartBeat(int id)
		{
			ID = id;
		}

		public void run()
		{
			try
			{
				while(true){
					Hdfs.HeartBeatRequest.Builder heartBeatRequestBuilder = Hdfs.HeartBeatRequest.newBuilder().setId(this.ID);
					Registry registry = LocateRegistry.getRegistry(NameNode_IP, 1099);
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
			DataNode obj = new DataNode(Integer.parseInt(args[0]));
			IDataNode stub = (IDataNode) UnicastRemoteObject.exportObject(obj, 0);
			
			Registry registry = LocateRegistry.getRegistry();
			registry.bind("DataNode", stub);

			HeartBeat heartBeat = new HeartBeat(obj.ID);
			heartBeat.start();


		}
		catch (Exception e)
		{
			System.err.println("Server exception: " + e.toString());
			e.printStackTrace();
		}
	}
}
