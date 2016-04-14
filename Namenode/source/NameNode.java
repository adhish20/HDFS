package Namenode.source;

import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import java.lang.*;

import java.io.*;
import java.util.*;

import Proto.Hdfs;
import com.google.protobuf.ByteString;
	
public class NameNode implements INameNode {

	private HashMap<Integer, String> mapHandle_FileName;
	private static HashMap<String, ArrayList<Integer>> mapFilename_Block;
	private static HashMap<Integer, ArrayList<Integer>> mapBlock_Datanode;
	public int blockNumber, fileNumber;
	private static int dataNodeNum = 3;
	private static String[] dataNodeIPs = {"54.174.162.89","54.174.129.146","54.88.112.53"};

	public NameNode()
	{
		blockNumber = 0;
		fileNumber = 0;
		mapHandle_FileName = new HashMap<Integer, String>();
		mapFilename_Block = new HashMap<String, ArrayList<Integer>>();
		mapBlock_Datanode = new HashMap<Integer, ArrayList<Integer>>();
	}
	
	public byte[] openFile(byte[] inp) throws RemoteException
	{
		try
		{
			Hdfs.OpenFileRequest openFileRequest =Hdfs.OpenFileRequest.parseFrom(inp);
			String filename = openFileRequest.getFileName();
			boolean forRead = openFileRequest.getForRead();

			byte[] openFileResponseBytes;
			mapHandle_FileName.put(fileNumber, filename);

			Hdfs.OpenFileResponse.Builder openFileResponse = Hdfs.OpenFileResponse.newBuilder();
			openFileResponse.setStatus(1);
			openFileResponse.setHandle(fileNumber);
			if(mapFilename_Block.get(filename)!=null)
				for(int i : mapFilename_Block.get(filename))
					openFileResponse.addBlockNums(i);
			fileNumber++;
			return openFileResponse.build().toByteArray();
		}
		catch(Exception e)
		{
			System.out.println("Unable to open file at name node\n");
		}
		return null;
	}

	public byte[] closeFile(byte[] inp) throws RemoteException
	{
		try
		{
			Hdfs.CloseFileRequest closeFileRequest = Hdfs.CloseFileRequest.parseFrom(inp);
			int handle = closeFileRequest.getHandle();


			File report = new File("fileList.txt");

			FileWriter fileWriter = new FileWriter(report.getName(), true);

			BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

			String filename = (String) mapHandle_FileName.get(handle);
			ArrayList<Integer> blockList = mapFilename_Block.get(filename);

			bufferedWriter.write(filename+" ");
			for(int i : blockList)
			{
				bufferedWriter.write(Integer.toString(i)+" ");
			}
			bufferedWriter.newLine();
			bufferedWriter.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return null;
	}

	public byte[] getBlockLocations(byte[] inp ) throws RemoteException
	{
		try
		{
			Hdfs.BlockLocationRequest blockLocationRequest = Hdfs.BlockLocationRequest.parseFrom(inp);
			int blockNumber = blockLocationRequest.getBlockNum();

			Hdfs.BlockLocationResponse.Builder blockLocationResponse = Hdfs.BlockLocationResponse.newBuilder().setStatus(1);
			Hdfs.BlockLocations.Builder blockLocations = Hdfs.BlockLocations.newBuilder().setBlockNumber(blockNumber);
			for (int node : mapBlock_Datanode.get(blockNumber))
			{
				Hdfs.DataNodeLocation.Builder dataNodeLocation = Hdfs.DataNodeLocation.newBuilder().setIp(dataNodeIPs[node]).setPort(1099);
				blockLocations.addLocations(dataNodeLocation.build());
			}
			blockLocationResponse.setBlockLocations(blockLocations.build());
			return blockLocationResponse.build().toByteArray();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return null;
	}

	public byte[] assignBlock(byte[] inp ) throws RemoteException
	{
		byte[] assignBlockResponseBytes = null;
		try
		{
			Hdfs.AssignBlockRequest assignBlockRequest = Hdfs.AssignBlockRequest.parseFrom(inp);
			int handle=assignBlockRequest.getHandle();
			String filename = (String) mapHandle_FileName.get(handle);
			if(mapFilename_Block.get(filename)!=null)
				mapFilename_Block.get(filename).add(blockNumber);
			else
				mapFilename_Block.put(filename, new ArrayList<Integer>(Arrays.asList(blockNumber)));

			Hdfs.BlockLocations.Builder blockLocations = Hdfs.BlockLocations.newBuilder();

			Hdfs.DataNodeLocation.Builder dataNodeLocation = Hdfs.DataNodeLocation.newBuilder();

			int DN1, DN2;
			Random r = new Random();
			DN1 = r.nextInt(dataNodeNum);
			do {
				DN2 = r.nextInt(dataNodeNum);
			} while(DN2==DN1);

			System.out.println(DN1 + " " + DN2);
			blockLocations.setBlockNumber(blockNumber);

			dataNodeLocation.setIp(dataNodeIPs[DN1]);
			dataNodeLocation.setPort(1099);
			blockLocations.addLocations(dataNodeLocation.build());

			dataNodeLocation.setIp(dataNodeIPs[DN2]);
			dataNodeLocation.setPort(1099);
			blockLocations.addLocations(dataNodeLocation.build());


			mapBlock_Datanode.put(blockNumber, new ArrayList<Integer>(Arrays.asList(DN1, DN2)));
			blockNumber++;

			Hdfs.AssignBlockResponse.Builder assignBlockResponse = Hdfs.AssignBlockResponse.newBuilder();
			assignBlockResponse.setStatus(1);
			assignBlockResponse.setNewBlock(blockLocations.build());
			assignBlockResponseBytes = assignBlockResponse.build().toByteArray();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return assignBlockResponseBytes;
	}

	public byte[] list(byte[] inp ) throws RemoteException
	{
		try
		{
			Hdfs.ListFilesResponse.Builder listFilesResponse = Hdfs.ListFilesResponse.newBuilder().setStatus(1);
			for(String fileName : mapFilename_Block.keySet())
			{
				listFilesResponse.addFileNames(fileName);
			}
			return listFilesResponse.build().toByteArray();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return null;
	}

	public byte[] blockReport(byte[] inp ) throws RemoteException
	{
		try
		{
			Hdfs.BlockReportRequest req = Hdfs.BlockReportRequest.parseFrom(inp);
			int datanode_id = req.getId();
			int num_blks = req.getBlockNumbersCount();

			for(int i=0;i<num_blks;i++)
			{
				if(mapBlock_Datanode.get(req.getBlockNumbers(i)) == null)
				{
					mapBlock_Datanode.put(req.getBlockNumbers(i), new ArrayList<Integer>(Arrays.asList(datanode_id)));
				}
				else
				{
					if (!mapBlock_Datanode.get(req.getBlockNumbers(i)).contains(datanode_id))
						mapBlock_Datanode.get(req.getBlockNumbers(i)).add(datanode_id);
				}
			}

			Hdfs.BlockReportResponse.Builder blockReportResponse = Hdfs.BlockReportResponse.newBuilder().addStatus(1);
			return blockReportResponse.build().toByteArray();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return null;
	}

	public byte[] heartBeat(byte[] inp ) throws RemoteException
	{
		try
		{
			Hdfs.HeartBeatRequest heartBeatRequest = Hdfs.HeartBeatRequest.parseFrom(inp);
			int id = heartBeatRequest.getId();
			System.err.println("HeartBeat received from DN : " + String.valueOf(id));

			Hdfs.HeartBeatResponse.Builder heartBeatResponse = Hdfs.HeartBeatResponse.newBuilder().setStatus(1);
			return heartBeatResponse.build().toByteArray();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String args[])
	{
		File report = new File("fileList.txt");
		try
		{
			NameNode obj = new NameNode();
			INameNode stub = (INameNode) UnicastRemoteObject.exportObject(obj, 0);
			
			Registry registry = LocateRegistry.getRegistry();
			registry.bind("NameNode", stub);
			report.createNewFile();
		}
		catch (Exception e)
		{
			System.err.println("Server exception: " + e.toString());
			e.printStackTrace();
		}
		try
		{
			BufferedReader br = new BufferedReader(new FileReader(report));
			String line, filename;
			filename = "";
			while ((line = br.readLine()) != null)
			{
				String[] fileBlocks = line.split(" ");
				filename = fileBlocks[0];
				ArrayList<Integer> blocks = new ArrayList<Integer>();
				for(int i=1;i<fileBlocks.length;i++)
					blocks.add(Integer.parseInt(fileBlocks[i]));
				mapFilename_Block.put(filename, blocks);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
