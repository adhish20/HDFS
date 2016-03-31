package Client.source;

import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import java.lang.*;

import java.io.*;
import java.util.*;

import Namenode.source.INameNode;
import Datanode.source.IDataNode;

import Proto.Hdfs;
import com.google.protobuf.ByteString;

public class Client
{
	private static String NameNode_IP = "54.174.209.93";
	private static String PUT = "put";
	private static String GET = "get";
	private static String LIST = "list";
	private static String EXIT = "exit";
	private static int blockSize = 33554432;

	public Client(){}

	public static void main(String args[])
	{
		Scanner scanner = new Scanner(System.in);
		try
		{
			Registry registry = LocateRegistry.getRegistry(NameNode_IP,1099);
			INameNode stub=(INameNode)registry.lookup("NameNode");
			while (true)
			{
				String command = scanner.next();
				if (command.equals(PUT))
				{
					String fileName = scanner.next();
					System.err.println(fileName);

					Hdfs.OpenFileRequest.Builder openFileRequest = Hdfs.OpenFileRequest.newBuilder();
					openFileRequest.setFileName(fileName);
					openFileRequest.setForRead(false);
					byte[] openFileRequestBytes = openFileRequest.build().toByteArray();
					byte[] openFileResponseBytes = stub.openFile(openFileRequestBytes);
					if(openFileResponseBytes!=null) 
					{
						Hdfs.OpenFileResponse openFileResponse = Hdfs.OpenFileResponse.parseFrom(openFileResponseBytes);

						byte[] readBytes = new byte[blockSize];

						FileInputStream input = new FileInputStream(new File(fileName));

						while (input.read(readBytes) != -1)
						{
							Hdfs.AssignBlockRequest.Builder assignBlockRequest = Hdfs.AssignBlockRequest.newBuilder();
							assignBlockRequest.setHandle(openFileResponse.getHandle());
							byte[] assignBlockRequestBytes = assignBlockRequest.build().toByteArray();
							byte[] assignBlockResponseBytes = stub.assignBlock(assignBlockRequestBytes);
							Hdfs.AssignBlockResponse assignBlockResponse = Hdfs.AssignBlockResponse.parseFrom(assignBlockResponseBytes);
							Hdfs.BlockLocations blockLocation = assignBlockResponse.getNewBlock();

							Registry DataNode_registry = LocateRegistry.getRegistry(blockLocation.getLocations(0).getIp(),1099);
							IDataNode dnStub=(IDataNode)DataNode_registry.lookup("DataNode");

							Hdfs.WriteBlockRequest.Builder writeBlockRequest = Hdfs.WriteBlockRequest.newBuilder();
							writeBlockRequest.addData(ByteString.copyFrom(readBytes));
							writeBlockRequest.setBlockInfo(blockLocation);
							writeBlockRequest.setReplicate(true);
							byte[] writeBlockResponseBytes = dnStub.writeBlock(writeBlockRequest.build().toByteArray());
							Hdfs.WriteBlockResponse writeBlockResponse = Hdfs.WriteBlockResponse.parseFrom(writeBlockResponseBytes);
							System.err.println("Block Written");
						}
						System.err.println("File Written");
						Hdfs.CloseFileRequest.Builder closeFileRequest = Hdfs.CloseFileRequest.newBuilder();
						closeFileRequest.setHandle(openFileResponse.getHandle());
						stub.closeFile(closeFileRequest.build().toByteArray());
						System.err.println("PUT Successful");
					}
				}
				else if (command.equals(GET))
				{
					String fileName = scanner.next();
					System.err.println("Trying to get : "+fileName);

					Hdfs.OpenFileRequest.Builder openFileRequest = Hdfs.OpenFileRequest.newBuilder();
					openFileRequest.setFileName(fileName);
					openFileRequest.setForRead(true);
					byte[] openFileRequestBytes = openFileRequest.build().toByteArray();
					byte[] openFileResponseBytes = stub.openFile(openFileRequestBytes);
					
					if(openFileResponseBytes != null)
					{
						Hdfs.OpenFileResponse openFileResponse = Hdfs.OpenFileResponse.parseFrom(openFileResponseBytes);
						//int handle = openFileResponse.getHandle();
						int blockCount = openFileResponse.getBlockNumsCount();

						File file = new File(fileName);
						FileOutputStream fileStream = new FileOutputStream(file);

						for(int i=0;i<blockCount;i++)
						{
							Hdfs.BlockLocationRequest.Builder blr_builder = Hdfs.BlockLocationRequest.newBuilder().setBlockNum(openFileResponse.getBlockNums(i));
							byte[] response = stub.getBlockLocations(blr_builder.build().toByteArray());

							Hdfs.BlockLocationResponse blockLocationResponse = Hdfs.BlockLocationResponse.parseFrom(response);
							if(blockLocationResponse.getStatus() == 1)
							{
								Hdfs.BlockLocations blockLocations = blockLocationResponse.getBlockLocations();

								Registry reg = LocateRegistry.getRegistry(blockLocations.getLocations(0).getIp(),1099);
								IDataNode dnStub = (IDataNode) reg.lookup("DataNode");

								Hdfs.ReadBlockRequest.Builder readBlockRequest = Hdfs.ReadBlockRequest.newBuilder().setBlockNumber(openFileResponse.getBlockNums(i));
								byte[] resp = dnStub.readBlock(readBlockRequest.build().toByteArray());

								Hdfs.ReadBlockResponse readBlockResponse = Hdfs.ReadBlockResponse.parseFrom(resp);
								ByteString data = readBlockResponse.getData(0);
								fileStream.write(data.toByteArray());
							}
						}
						fileStream.close();
						System.err.println("GET Successful");
					}
				}
				else if (command.equals(LIST))
				{
					System.err.println(LIST);
					try
					{
						byte[] response = stub.list(null);
						Hdfs.ListFilesResponse listFilesResponse = Hdfs.ListFilesResponse.parseFrom(response);
						for(String fileName : listFilesResponse.getFileNamesList())
							System.err.println(fileName);
						System.err.println("LIST Successful");
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
				}
				else if (command.equals(EXIT))
				{
					break;
				}
				else
				{
					System.err.println("Invalid command");
					System.err.println("Commands Allowed are :");
					System.err.println("put <fileName>");
					System.err.println("get <fileName>");
					System.err.println("list");
					System.err.println("exit");
				}
			}
		}
		catch (Exception e)
		{
			System.err.println("Client exception: " + e.toString());
			e.printStackTrace();
		}
	}
}