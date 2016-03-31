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
	private static final int blockSize = 33554432;

	public DataNode(int id)
	{
		ID = id;
	}

	public byte[] readBlock(byte[] inp) throws RemoteException
	{
		File dir = new File("Blocks");
		Hdfs.ReadBlockResponse.Builder readBlockResponse = Hdfs.ReadBlockResponse.newBuilder().setStatus(1);
		try
		{
			int blockNumber = Hdfs.ReadBlockRequest.parseFrom(inp).getBlockNumber();
			File block = new File(dir, String.valueOf(blockNumber));
			FileInputStream fileInputStream = new FileInputStream(block);
			byte[] blk = new byte[blockSize];
			int bytes;

			while((bytes = fileInputStream.read(blk)) != -1)
			{
				ByteString data = ByteString.copyFrom(blk);
				readBlockResponse.addData(data);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return readBlockResponse.build().toByteArray();
	}

	public byte[] writeBlock(byte[] inp) throws RemoteException
	{
		try
		{
			File dir = new File("Blocks");
			Hdfs.WriteBlockRequest writeBlockRequest = Hdfs.WriteBlockRequest.parseFrom(inp);

			int blockNumber = writeBlockRequest.getBlockInfo().getBlockNumber();
			File blockFile = new File(dir, String.valueOf(blockNumber));
			FileOutputStream fileOutputStream = new FileOutputStream(blockFile);

			List<ByteString> dataString = writeBlockRequest.getDataList();
			for(ByteString byteString : dataString)
				fileOutputStream.write(byteString.toByteArray());

			fileOutputStream.close();

			File report = new File("BlockReport.txt");
			FileWriter fileWriter = new FileWriter(report.getName(), true);

			BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

			bufferedWriter.write(Integer.toString(blockNumber));
			bufferedWriter.newLine();
			bufferedWriter.close();

			if(writeBlockRequest.getReplicate() == true)
			{
				Registry DataNode_registry = LocateRegistry.getRegistry(writeBlockRequest.getBlockInfo().getLocations(1).getIp(),1099);
				IDataNode dnStub=(IDataNode)DataNode_registry.lookup("DataNode");

				Hdfs.WriteBlockRequest.Builder writeBlockReplicate = Hdfs.WriteBlockRequest.newBuilder();	
				for(ByteString byteString : dataString)
					writeBlockReplicate.addData(byteString);
				writeBlockReplicate.setBlockInfo(writeBlockRequest.getBlockInfo());
				writeBlockReplicate.setReplicate(false);
				byte[] writeBlockResponseBytes = dnStub.writeBlock(writeBlockReplicate.build().toByteArray());
				System.err.println("Block Replicated");
			}
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

					Hdfs.HeartBeatResponse heartBeatResponse = Hdfs.HeartBeatResponse.parseFrom(response);
					System.err.println("Heart Beat Response from NameNode " + String.valueOf(heartBeatResponse.getStatus()));
					Thread.sleep(10000);
				}
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}

	static class BlockReport extends Thread
	{
		private int ID;
		public BlockReport(int id)
		{
			ID = id;
		}

		public void run()
		{
			while(true)
			{
				File blkReport = new File("BlockReport.txt");
				String data = "";
				if (blkReport.exists() && blkReport.length()!=0)
				{
					try{
						BufferedReader br = new BufferedReader(new FileReader(blkReport));
						String blockNumber;
						Hdfs.BlockReportRequest.Builder blockReportRequest = Hdfs.BlockReportRequest.newBuilder().setId(this.ID);
						while((blockNumber = br.readLine())!=null)
						{
							blockReportRequest.addBlockNumbers(Integer.parseInt(blockNumber));
						}
						br.close();

						Registry registry = LocateRegistry.getRegistry(NameNode_IP,1099);
						INameNode stub = (INameNode) registry.lookup("NameNode");

						byte[] response = stub.blockReport(blockReportRequest.build().toByteArray());

						Hdfs.BlockReportResponse blockReportResponse = Hdfs.BlockReportResponse.parseFrom(response);
						System.err.println("Block Report Response from NameNode " + String.valueOf(blockReportResponse.getStatusCount()));
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
				}
				else
				{
					System.err.println("No block report");
				}
				try
				{
					Thread.sleep(10000);
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
		}
	}

	public static void main(String args[])
	{
		try
		{
			File blocks = new File("Blocks");
			if(!blocks.exists())
				blocks.mkdirs();

			DataNode obj = new DataNode(Integer.parseInt(args[0]));
			IDataNode stub = (IDataNode) UnicastRemoteObject.exportObject(obj, 0);
			
			Registry registry = LocateRegistry.getRegistry();
			registry.bind("DataNode", stub);

			HeartBeat heartBeat = new HeartBeat(obj.ID);
			heartBeat.start();

			BlockReport report = new BlockReport(obj.ID);
			report.start();
		}
		catch (Exception e)
		{
			System.err.println("Server exception: " + e.toString());
			e.printStackTrace();
		}
	}
}
