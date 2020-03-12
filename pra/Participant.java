import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;


class ParticipantDetails
{
	 private int participantId;
	 private int participantPort;
	 private int coordinatorPort;
	 private  String coordinatorIp;
	 private String logFileName;
	public int getParticipantId() {
		return participantId;
	}
	public void setParticipantId(int participantId) {
		this.participantId = participantId;
	}
	public int getParticipantPort() {
		return participantPort;
	}
	public void setParticipantPort(int participantPort) {
		this.participantPort = participantPort;
	}
	public int getCoordinatorPort() {
		return coordinatorPort;
	}
	public void setCoordinatorPort(int coordinatorPort) {
		this.coordinatorPort = coordinatorPort;
	}
	public String getCoordinatorIp() {
		return coordinatorIp;
	}
	public void setCoordinatorIp(String coordinatorIp) {
		this.coordinatorIp = coordinatorIp;
	}
	public String getLogFileName() {
		return logFileName;
	}
	public void setLogFileName(String logFileName) {
		this.logFileName = logFileName;
	}

	public String toString() {
        return "ParticipantDetails: participantId=" + participantId + ", participantPort=" + participantPort
				+ ", coordinatorPort=" + coordinatorPort + ", coordinatorIp=" + coordinatorIp + ", logFileName="
				+ logFileName;
	}
	 
}
public class Participant {

	public static void main(String[] args) throws IOException {
		
		String fileName=args[0];
		Participant participant=new Participant();
		participant.readFile(fileName);
	}

	private void readFile(String fileName) throws IOException {
		int count =0;
		
		FileInputStream fstream = new FileInputStream(fileName);
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
		ParticipantDetails details=new ParticipantDetails();
        String strID;
        String strFile;
        String strIpPort;
       
        strID = br.readLine();
        details.setParticipantId(Integer.parseInt(strID));
        
        strFile =br.readLine();
        details.setLogFileName(strFile);
        
        strIpPort =br.readLine();
        String[] coordinatorDetails=strIpPort.split(" ");
        details.setCoordinatorIp(coordinatorDetails[0]);
        details.setCoordinatorPort(Integer.parseInt(coordinatorDetails[1]));
        
		System.out.println(details);

		br.close();
		new Thread(new ParticipantSender(details)).start();
	}
}

class ParticipantSender implements Runnable
{
   ParticipantDetails details;
   Socket socket;
   DataInputStream inputStream;
   DataOutputStream outputStream;
   Set<Integer> valSet=new HashSet<>();
	public ParticipantSender(ParticipantDetails details) {
		this.details=details;
	}

	public void run() {
		connectToServer();
	}
    ////////////// five different methods ////////////////////
	private void connectToServer() {
		boolean reconnect=false;
		try {
			socket=new Socket(details.getCoordinatorIp(), details.getCoordinatorPort());
			outputStream=new DataOutputStream(socket.getOutputStream());
			inputStream=new DataInputStream(socket.getInputStream());
		Scanner sc=new Scanner(System.in);
		String line="command :";
		while(true)
		{
			System.out.println(line);
			String command=sc.nextLine();
			if(command.startsWith("register"))
			{
				registerCoordinator(command);
			}
			while(true)
			{
				System.out.println(line);
				 command=sc.nextLine();
				 if(command.startsWith("deregister"))
					{
						deregisterCoordinator(command);
						break;
					}
					else if(command.startsWith("reconnect"))
					{
					 reconnectCoordinator(command);
					}
					else if(command.startsWith("disconnect"))
					{
						disconnectCoordinator(command);
					}
					else if(command.startsWith("msend"))
					{
						sendMessage(command);
					}
			}
		}
		
	}
	catch (IOException e) {
			e.printStackTrace();
		}
		
	}
    ////////////////// register method ///////////////////////
    private void registerCoordinator(String command) {
        try {
            String[] contents=command.split(" ");
            //commend
            outputStream.writeUTF(contents[0]);
            //port
            outputStream.writeInt(Integer.parseInt(contents[1]));
            //ip
            outputStream.writeUTF("localhost");
            //partId
            outputStream.writeInt(details.getParticipantId());
            valSet.add(details.getParticipantId());
            
            new Thread(new ParticipantReceiver(Integer.parseInt(contents[1]),details.getLogFileName(),valSet,details)).start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    ////////////////// deregister method ///////////////////////
    private void deregisterCoordinator(String command) {
        try {
            outputStream.writeUTF(command);
            outputStream.writeInt(details.getParticipantId());
            valSet.remove(details.getParticipantId());
            
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    ////////////////// disconnect method ///////////////////////
    private void disconnectCoordinator(String command) {
        try {
            outputStream.writeUTF(command);
            outputStream.writeInt(details.getParticipantId());
            valSet.remove(details.getParticipantId());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    ////////////////// reconnect method ///////////////////////
    private void reconnectCoordinator(String command) {
        try {
            String[] contents=command.split(" ");
            
            outputStream.writeUTF(contents[0]);
            outputStream.writeInt(details.getParticipantId());
            
            outputStream.writeInt(Integer.parseInt(contents[1]));
            
            
            valSet.add(details.getParticipantId());
            
            new Thread(new ParticipantReceiver(Integer.parseInt(contents[1]),details.getLogFileName(),valSet,details)).start();
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    ////////////////// msend method ///////////////////////
	private void sendMessage(String command) {
		try {
			outputStream.writeUTF(command);
			String ackno=inputStream.readUTF();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

class ParticipantReceiver implements Runnable
{
    int port;
    String fileName;
    Set<Integer> valSet;
    ParticipantDetails details;
	public ParticipantReceiver(int port, String fileName, Set<Integer> valSet, ParticipantDetails details) {
		this.port=port;
		this.fileName=fileName;
		this.valSet=valSet;
		this.details=details;
	}


	public void run() {
         try {
        	  System.out.println("Thread Started");
			  ServerSocket serverSocket=new ServerSocket(port);
		      File file = new File(fileName);

			while(valSet.contains(details.getParticipantId()))
			{
                try {
                    Socket socket=serverSocket.accept();
                    DataInputStream input=new DataInputStream(socket.getInputStream());
                    String message=(String) input.readUTF();

				if(message.startsWith("msend"))
				{
					System.out.println(message.split(" ")[1]);
				    FileWriter writer = new FileWriter(file,true); 

					writer.write(message +"\n");
					socket.close();
					writer.close();
				}
				else
				{   input.close();
					socket.close();
					break;
				}
				
             } catch(Exception e)
                {
                e.printStackTrace();
                }
			}
			serverSocket.close();
			System.out.println("Client exited");
			
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
}
