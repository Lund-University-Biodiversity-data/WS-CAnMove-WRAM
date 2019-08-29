package se.lu.canmove.ws;

import java.beans.XMLDecoder;
import java.beans.XMLEncoder;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.GregorianCalendar;

public class DataProvider {
	
	private GregorianCalendar cal = new GregorianCalendar();
	private String logTime =
			cal.get(GregorianCalendar.YEAR)+"-"+
			String.format("%02d",(cal.get(GregorianCalendar.MONTH)+1))+"-"+
			String.format("%02d",cal.get(GregorianCalendar.DAY_OF_MONTH))+" "+
			String.format("%02d",cal.get(GregorianCalendar.HOUR_OF_DAY))+":"+
			String.format("%02d",cal.get(GregorianCalendar.MINUTE))+":"+
			String.format("%02d",cal.get(GregorianCalendar.SECOND));
	private String dmpTime = logTime.replace(':', '.').replace(' ', '_');
	
	private String WSDir = "/home/sys/canmove/ws";
	private String xmlFileName = WSDir+"/DataProvider.xml";
	private String logFileName = WSDir+"/DataProvider.log";
	private String dmpFileName = WSDir+"/log/DP."+dmpTime;
	private boolean logRequest = false;
	private boolean logDetails = false;
	private String classPath = "DataProviderSender.jar:postgresql-42.1.1.jar";
	private String className = "se.lu.canmove.ws.DataProviderSender";

	public String RunDataProviderConnectorQuery (String[] query, String userId, int portNr, int timeout) {
		boolean ok = readConfig();
		if (!ok) ok = writeConfig();
		if (logRequest) writeLogEntry(userId);
		if (logDetails) writeParFile(query, userId, portNr, timeout);
		
		String cmdArray[] = new String[8];
		ProcessBuilder pb = new ProcessBuilder();
		cmdArray[0] = "java";
		cmdArray[1] = "-classpath";
		cmdArray[2] = classPath;
		cmdArray[3] = className;
		cmdArray[4] = concatQuery (query);
		cmdArray[5] = userId;
		cmdArray[6] = Integer.toString(portNr);
		cmdArray[7] = Integer.toString(timeout);
		pb.command(cmdArray);

		File wd = new File (WSDir);
		pb.directory(wd);
		try {
			@SuppressWarnings("unused")
			Process p = pb.start();
		} catch (Exception e) {
			writeStackTrace(e);
		}
        return ("A sender process has been initiated.");
	}
	
	private String concatQuery (String[] query) {
		int i;
		String s = "";
		for (i=0;i<query.length;i++) {
			if (i!=0) s+="/";
			if (query[i]==null)
				s+="";
			else
				s+=query[i];
		}
		return (s);
	}

	private boolean readConfig() {
		try (
			FileInputStream fis = new FileInputStream(xmlFileName);
			BufferedInputStream bis = new BufferedInputStream(fis);
			XMLDecoder decoder = new XMLDecoder(bis);
			) {
			logRequest = (boolean)decoder.readObject();
			logDetails = (boolean)decoder.readObject();
			classPath = (String)decoder.readObject();
			className = (String)decoder.readObject();
			decoder.close();
			bis.close();
			fis.close();
			return (true);
		} catch (Exception e) {
			return (false);
		}
	}

	private boolean writeConfig() {
		try (
			FileOutputStream fos = new FileOutputStream(xmlFileName);
			BufferedOutputStream bos = new BufferedOutputStream(fos);
			XMLEncoder encoder = new XMLEncoder(bos);
			) {
			encoder.writeObject(logRequest);
			encoder.writeObject(logDetails);
			encoder.writeObject(classPath);
			encoder.writeObject(className);
			encoder.close();
			bos.close();
			fos.close();
			return (true);
		} catch (Exception e) {
			writeStackTrace(e);
			return (false);
		}
	}

	private void writeLogEntry (String userId) {
		try (
			FileWriter fw = new FileWriter(logFileName, true);
			BufferedWriter bw = new BufferedWriter(fw);
			PrintWriter logFile=new PrintWriter(bw);
			) {
    		logFile.println(logTime+" "+userId);
            logFile.close();
            bw.close();
            fw.close();
		} catch (Exception e) {
			writeStackTrace(e);
		}
	}

	private void writeParFile (String[] query, String userId, int portNr, int timeout) {
		try (
            PrintWriter parFile=new PrintWriter(dmpFileName+".par");
			) {
			parFile.println (String.format("query array size=%d", query.length));
    		parFile.println("----");
    		for (int i=0; i<query.length; i++)
    			parFile.println(String.format("%d: ",i)+query[i]);
    		parFile.println("----");
    		parFile.println(String.format("userId: %s", userId));
    		parFile.println(String.format("portNr: %d", portNr));
    		parFile.println(String.format("timeout: %d", timeout));
    		parFile.println("----");
    		parFile.println(concatQuery (query));
            parFile.close();
		} catch (Exception e) {
			writeStackTrace(e);
		}
	}

	private void writeStackTrace (Exception e) {
		try (
			PrintStream trcFile = new PrintStream (dmpFileName+".trc");
			) {
			e.printStackTrace(trcFile);
			trcFile.close();
		} catch (IOException E) {
			// Give up
		}
	}

}