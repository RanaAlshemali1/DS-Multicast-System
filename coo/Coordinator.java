import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


class CoordinatorDetails
{
	private int threshold;
	private int portNumber;
	
	public int getThreshold() {
		return threshold;
	}
	public void setThreshold(int threshold) {
		this.threshold = threshold;
	}
	@Override
	public String toString() {
		return "CoordinatorDetails [threshold=" + threshold + ", portNumber=" + portNumber + "]";
	}
	public int getPortNumber() {
		return portNumber;
	}
	public void setPortNumber(int portNumber) {
		this.portNumber = portNumber;
	}
}
public class Coordinator {
    
	public static void main(String[] args) throws IOException {
		String fileName=args[0];
		Coordinator coordinator=new Coordinator();
		coordinator.readFile(fileName);
	}

	private void readFile(String fileName) throws IOException {
		FileInputStream fstream = new FileInputStream(fileName);
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
		String strPort;
        String strThreshold;
		CoordinatorDetails details=new CoordinatorDetails();
        
        strPort = br.readLine();
        details.setPortNumber(Integer.parseInt(strPort));
        
        strThreshold = br.readLine();
        details.setThreshold(Integer.parseInt(strThreshold));
        
     launchCoordinator(details);		
}

	private void launchCoordinator(CoordinatorDetails details) {

		try {
			ServerSocket serverSocket=new ServerSocket(details.getPortNumber());
			ServiceHandler handler=new ServiceHandler();
			while(true)
			{
				Socket socket=serverSocket.accept();
				System.out.println("Connection Accepted");
				new Thread(new CoordinatorHandler(socket,handler,details)).start();
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}

class CoordinatorHandler implements Runnable
{
	Socket socket;
	DataInputStream inputStream;
	DataOutputStream outputStream;
	ServiceHandler handler;
	CoordinatorDetails details;

	public CoordinatorHandler(Socket socket, ServiceHandler handler, CoordinatorDetails details) {
		this.socket=socket;
		this.handler=handler;
		this.details=details;
		try {
			outputStream=new DataOutputStream(this.socket.getOutputStream());
			inputStream=new DataInputStream(this.socket.getInputStream());

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public void run() {
		handleCommands();
	}
    //////////////////// five different methods  ////////////////////
	private void handleCommands() {
		while(true)
		{
			try {
				String command=inputStream.readUTF();
				if(command.startsWith("register"))
				{
                    System.out.println("Client Registrd");
					registerCoordinator();
				}
				else if(command.startsWith("deregister"))
				{
					deregisterCoordinator();
				}
				else if(command.startsWith("disconnect"))
				{
					disconnectCoordinator();
					
				}
				else if(command.startsWith("reconnect"))
				{
					reconnectCoordinator();
				}
				else if(command.startsWith("msend"))
				{
					broadCastMessage(command);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
    //////////////////// register  ////////////////////
    private void registerCoordinator() {
        try {
            int coordinatorPort=inputStream.readInt();
            String coordinatorIp=inputStream.readUTF();
            int coordinatorId=inputStream.readInt();
            handler.registerEntry(coordinatorPort,coordinatorIp,coordinatorId);
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }
    
    //////////////////// deregister  ////////////////////
    private void deregisterCoordinator() {
        try {
            int coordinatorId=inputStream.readInt();
            Socket coordinatorSocket=new Socket(handler.getCoordinatorIpMap().get(coordinatorId), handler.getCoordinatorPortMap().get(coordinatorId));
            DataOutputStream dataStream=new DataOutputStream(coordinatorSocket.getOutputStream());
            dataStream.writeUTF("break");
            handler.deregisterCoordinator(coordinatorId);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }    
    //////////////////// disconnect  ////////////////////
    private void disconnectCoordinator() {
        try {
            int coordinatorId=inputStream.readInt();
            Socket coordinatorSocket=new Socket(handler.getCoordinatorIpMap().get(coordinatorId), handler.getCoordinatorPortMap().get(coordinatorId));
            DataOutputStream dataStream=new DataOutputStream(coordinatorSocket.getOutputStream());
            dataStream.writeUTF("break");
            dataStream.close();
            coordinatorSocket.close();
            handler.disconnectCoordinator(coordinatorId);
            System.out.println("Disconnected");
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    //////////////////// reconnect  ////////////////////
    private void reconnectCoordinator() {
        try {
            int coordinatorId=inputStream.readInt();
            int port=inputStream.readInt();
            List<String> messages=new ArrayList<>();
            messages=handler.reconnectConnection(coordinatorId,details.getThreshold(),port);
            
            for(String message:messages)
            {
                Socket coordinatorSocket=new Socket(handler.getCoordinatorIpMap().get(coordinatorId), handler.getCoordinatorPortMap().get(coordinatorId));
                DataOutputStream dataStream=new DataOutputStream(coordinatorSocket.getOutputStream());
                dataStream.writeUTF(message);
                
            }
            System.out.println("reconnected");
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //////////////////// msend  ////////////////////
	private void broadCastMessage(String message) {
		try {
			outputStream.writeUTF("proceed");
		} catch (IOException e1) {
			e1.printStackTrace();
		} 
		for(Map.Entry<Integer, String> mapStat:handler.getCoordinatorStatusMap().entrySet())
		{
			if(!mapStat.getValue().equals("disconnected"))
			{
				try {
					Socket coordinatorSocket=new Socket(handler.getCoordinatorIpMap().get(mapStat.getKey()), handler.getCoordinatorPortMap().get(mapStat.getKey()));
					DataOutputStream dataStream=new DataOutputStream(coordinatorSocket.getOutputStream());
					dataStream.writeUTF(message);
					dataStream.flush();
					dataStream.close();
					coordinatorSocket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			else
			{
				handler.reserveMessage(mapStat.getKey(),message);
			}
		}
	}	
}

class ServiceHandler
{
	private Map<Integer,String> coordinatorStatusMap=new HashMap<>();
	private Map<Integer,Integer> coordinatorPortMap=new HashMap<>();
	private Map<Integer,String> coordinatorIpMap=new HashMap<>();
	private Map<Integer,LinkedHashMap<Date, String>> reservedMessagesMap=new HashMap<>();
	public void registerEntry(int coordinatorPort, String coordinatorIp, int id) {
		this.coordinatorStatusMap.put(id,"registered");
		this.coordinatorPortMap.put(id, coordinatorPort);
		this.coordinatorIpMap.put(id, coordinatorIp);
		
	}
	public List<String> reconnectConnection(int id, int threshold, int port) {
		List<String> messages=new ArrayList<>();
		LinkedHashMap<Date, String> messagesMap=this.reservedMessagesMap.get(id);
		this.coordinatorStatusMap.put(id, "resgistered");
		this.reservedMessagesMap.remove(id);
		this.coordinatorPortMap.put(id, port);
		if(messagesMap!=null) {
		for(Map.Entry<Date, String> curr:messagesMap.entrySet())
		{
			System.out.println("Difference in seconds "+(new Date().getTime()-curr.getKey().getTime())/1000);
			if((new Date().getTime()-curr.getKey().getTime())/1000 <=threshold)
			{
				System.out.println(curr.getValue());
				messages.add(curr.getValue());
			}
		}
		}
		return messages;
	}
	public void reserveMessage(Integer key, String message) {
		if(reservedMessagesMap.get(key)!=null)
		{
			LinkedHashMap<Date, String> messageMap=reservedMessagesMap.get(key);
			messageMap.put(new Date(), message);
			this.reservedMessagesMap.put(key, messageMap);
		}
		else
		{
			LinkedHashMap<Date,String> messageMap=new LinkedHashMap<>();
			messageMap.put(new Date(), message);
			this.reservedMessagesMap.put(key, messageMap);

		}
		
	}
	public void disconnectCoordinator(int id) {
		this.coordinatorStatusMap.put(id, "disconnected");
	}
	public Map<Integer, String> getCoordinatorStatusMap() {
		return coordinatorStatusMap;
	}
	public void setCoordinatorStatusMap(Map<Integer, String> coordinatorStatusMap) {
		this.coordinatorStatusMap = coordinatorStatusMap;
	}
	public Map<Integer, Integer> getCoordinatorPortMap() {
		return coordinatorPortMap;
	}
	public void setCoordinatorPortMap(Map<Integer, Integer> coordinatorPortMap) {
		this.coordinatorPortMap = coordinatorPortMap;
	}
	public Map<Integer, String> getCoordinatorIpMap() {
		return coordinatorIpMap;
	}
	public void setCoordinatorIpMap(Map<Integer, String> coordinatorIpMap) {
		this.coordinatorIpMap = coordinatorIpMap;
	}
	public Map<Integer, LinkedHashMap<Date, String>> getReservedMessagesMap() {
		return reservedMessagesMap;
	}
	public void setReservedMessagesMap(Map<Integer, LinkedHashMap<Date, String>> reservedMessagesMap) {
		this.reservedMessagesMap = reservedMessagesMap;
	}
	public void deregisterCoordinator(int id) {
		this.coordinatorIpMap.remove(id);
		this.coordinatorPortMap.remove(id);
		this.coordinatorStatusMap.remove(id);
		this.reservedMessagesMap.remove(id);
	}
}
