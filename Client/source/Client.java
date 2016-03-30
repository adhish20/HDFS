package Client.source;

import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import java.lang.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class Client
{
	public static void main(String args[])
	{
		try
		{  
			INameNode stub=(INameNode)Naming.lookup();  
		}
		catch (Exception e)
		{

		}
		while(true)
		{

		}
	}
}