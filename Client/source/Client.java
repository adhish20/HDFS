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
	private static String GET = "get";
	private static String PUT = "put";
	private static String LIST = "list";
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
				if (command.equals(GET))
				{
					String fileName = scanner.next();
					System.err.println(fileName);

					Hdfs.OpenFileRequest.Builder openFileRequestBuilder = Hdfs.OpenFileRequest.newBuilder();
					openFileRequestBuilder.setFileName(fileName);
					openFileRequestBuilder.setForRead(true);
					byte[] openFileRequestBytes = openFileRequestBuilder.build().toByteArray();
					byte[] openFileResponseBytes = stub.openFile(openFileRequestBytes);
					
					if(openFileResponseBytes != null)
					{
						Hdfs.OpenFileResponse ofr = Hdfs.OpenFileResponse.parseFrom(openFileResponseBytes);
						int handle = ofr.getHandle();
						int blk_count = ofr.getBlockNumsCount();

						File file = new File(fileName);
						FileOutputStream fs = new FileOutputStream(file);

						for(int i=0;i<blk_count;i++)
						{
							Hdfs.BlockLocationRequest.Builder blr_builder = Hdfs.BlockLocationRequest.newBuilder().setBlockNum(ofr.getBlockNums(i));
							byte[] res = stub.getBlockLocations(blr_builder.build().toByteArray());

							Hdfs.BlockLocationResponse blr = Hdfs.BlockLocationResponse.parseFrom(res);
							if(blr.getStatus() == 1)
							{
								Hdfs.BlockLocations bl = blr.getBlockLocations();
								String DN_IP = bl.getLocations(0).getIp();
								Registry reg = LocateRegistry.getRegistry(DN_IP,1099);
								IDataNode stub1 = (IDataNode) reg.lookup("DataNode");

								Hdfs.ReadBlockRequest.Builder rbr_builder = Hdfs.ReadBlockRequest.newBuilder().setBlockNumber(ofr.getBlockNums(i));
								byte[] resp = stub1.readBlock(rbr_builder.build().toByteArray());

								Hdfs.ReadBlockResponse rbr = Hdfs.ReadBlockResponse.parseFrom(resp);
								ByteString data = rbr.getData(0);
								fs.write(data.toByteArray());
							}
						}
						fs.close();
					}
				}

				else if (command.equals(PUT))
				{
					String fileName = scanner.next();
					System.err.println(fileName);

					Hdfs.OpenFileRequest.Builder openFileRequestBuilder = Hdfs.OpenFileRequest.newBuilder();
					openFileRequestBuilder.setFileName(fileName);
					openFileRequestBuilder.setForRead(false);
					byte[] openFileRequestBytes = openFileRequestBuilder.build().toByteArray();
					byte[] openFileResponseBytes = stub.openFile(openFileRequestBytes);
					if(openFileResponseBytes!=null) 
					{
						Hdfs.OpenFileResponse openFileResponse = Hdfs.OpenFileResponse.parseFrom(openFileResponseBytes);

						byte[] readBytes = new byte[blockSize];
						int numBytes;

						FileInputStream input = new FileInputStream(new File(fileName));

						while ((numBytes = input.read(readBytes)) != -1)
						{
							Hdfs.AssignBlockRequest.Builder assignBlockRequestBuilder = Hdfs.AssignBlockRequest.newBuilder();
							assignBlockRequestBuilder.setHandle(openFileResponse.getHandle());
							byte[] assignBlockRequestBytes = assignBlockRequestBuilder.build().toByteArray();
							byte[] assignBlockResponseBytes = stub.assignBlock(assignBlockRequestBytes);
							Hdfs.AssignBlockResponse assignBlockResponse = Hdfs.AssignBlockResponse.parseFrom(assignBlockResponseBytes);
							Hdfs.BlockLocations blockLocation = assignBlockResponse.getNewBlock();

							for( Hdfs.DataNodeLocation dataNodeLocation : blockLocation.getLocationsList() )
							{
								Registry DataNode_registry = LocateRegistry.getRegistry(dataNodeLocation.getIp(),1099);
								IDataNode dnStub=(IDataNode)DataNode_registry.lookup("DataNode");

								Hdfs.WriteBlockRequest.Builder writeBlockRequestBuilder = Hdfs.WriteBlockRequest.newBuilder();
								writeBlockRequestBuilder.addData(ByteString.copyFrom(readBytes));
								writeBlockRequestBuilder.setBlockInfo(blockLocation);
								byte[] writeBlockResponseBytes = dnStub.writeBlock(writeBlockRequestBuilder.build().toByteArray());
								Hdfs.WriteBlockResponse writeBlockResponse = Hdfs.WriteBlockResponse.parseFrom(writeBlockResponseBytes);
							}
						}
						Hdfs.CloseFileRequest.Builder closeFileRequestBuilder = Hdfs.CloseFileRequest.newBuilder();
						closeFileRequestBuilder.setHandle(openFileResponse.getHandle());
						stub.closeFile(closeFileRequestBuilder.build().toByteArray());
					}
				}
				else if (command.equals(LIST))
				{
					System.err.println(LIST);
					try
					{
						Registry NN_Registry = LocateRegistry.getRegistry(NameNode_IP,1099);
						INameNode stub1 = (INameNode) NN_Registry.lookup("NameNode");
						byte[] res = stub1.list(null);
						Hdfs.ListFilesResponse resp = Hdfs.ListFilesResponse.parseFrom(res);
						for(String filename : resp.getFileNamesList())
							System.err.println(filename);
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
				}
				else
				{
					System.err.println("Invalid command");
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