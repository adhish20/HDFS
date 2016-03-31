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
		File dir = new File("Blocks");
		Hdfs.ReadBlockResponse.Builder rbr_builder = Hdfs.ReadBlockResponse.newBuilder().setStatus(1);
		try
		{
			int blk_num = Hdfs.ReadBlockRequest.parseFrom(inp).getBlockNumber();
			File block = new File(dir, String.valueOf(blk_num));
			FileInputStream fis = new FileInputStream(block);
			byte[] blk = new byte[block_size];
			int bytes;

			while((bytes = fis.read(blk)) != -1)
			{
				ByteString data = ByteString.copyFrom(blk);
				rbr_builder.addData(data);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return rbr_builder.build().toByteArray();
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

			if(writeBlockRequest.getReplicate() == true)
			{
				Registry DataNode_registry = LocateRegistry.getRegistry(writeBlockRequest.getBlockInfo().getLocations(1).getIp(),1099);
				IDataNode dnStub=(IDataNode)DataNode_registry.lookup("DataNode");

				Hdfs.WriteBlockRequest.Builder writeBlockRequestBuilder = Hdfs.WriteBlockRequest.newBuilder();	
				for(ByteString byteString : dataString)
					writeBlockRequestBuilder.addData(byteString);
				writeBlockRequestBuilder.setBlockInfo(writeBlockRequest.getBlockInfo());
				writeBlockRequestBuilder.setReplicate(false);
				byte[] writeBlockResponseBytes = dnStub.writeBlock(writeBlockRequestBuilder.build().toByteArray());
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
				File blk_rpt = new File("BlockReport.txt");
				String data = "";
				if (blk_rpt.exists() && blk_rpt.length()!=0)
				{
					try{
						BufferedReader br = new BufferedReader(new FileReader(blk_rpt));
						String blk_num;
						Hdfs.BlockReportRequest.Builder blk_rpt_req_builder = Hdfs.BlockReportRequest.newBuilder().setId(this.ID);
						while((blk_num = br.readLine())!=null)
						{
							blk_rpt_req_builder.addBlockNumbers(Integer.parseInt(blk_num));
						}
						br.close();

						Registry registry = LocateRegistry.getRegistry(NameNode_IP,1099);
						INameNode stub = (INameNode) registry.lookup("NameNode");
						byte[] blockReportResponse = stub.blockReport(blk_rpt_req_builder.build().toByteArray());

						Hdfs.BlockReportResponse res = Hdfs.BlockReportResponse.parseFrom(blockReportResponse);
						System.err.println("Block Report Response from NameNode " + String.valueOf(res.getStatusCount()));
						Thread.sleep(10000);
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
				}
				else
				{
					System.err.println("No block report");
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
