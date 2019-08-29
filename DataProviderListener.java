package se.lu.canmove.ws;

import java.beans.XMLDecoder;
import java.beans.XMLEncoder;
import java.io.*;
import java.util.GregorianCalendar;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.GZIPInputStream;
import java.net.*;

public class DataProviderListener {

	private GregorianCalendar cal;
	private String logTime;
	private String dmpTime;

	private String WSDir = "/home/sys/canmove/ws";
	private String xmlFileName = WSDir+"/DataProviderListener.xml";
	private String logFileName = WSDir+"/DataProviderListener.log";
	private String dmpFileName = "";
	private boolean logRequest = false;
	private boolean logDetails = false;

	private int portNr = 0;
	@SuppressWarnings("unused")
	private int timeout = 0;

	private final int MD5hashSize = 32;
    private final int fileLengthSize = 16;
    private final int streamVersionSize = 4;
    private final int streamTypeSize = 4;
    private final int headerSize = MD5hashSize + fileLengthSize + streamVersionSize + streamTypeSize; 
    private int fileLength = 0;
    @SuppressWarnings("unused")
	private int streamVersion = 0;
    private int streamType = 0;

    private byte[] header = new byte[headerSize];
    private StringBuilder data = new StringBuilder();
    private byte[] zipData = null;
    private String checksum;

    ServerSocket serverSocket;
	Socket socket;
	
	public static void main(String[] args) {
		DataProviderListener DPL = new DataProviderListener();
		DPL.run (args);
	}

	public void run(String[] args) {
		boolean ok = readConfig();
		if (!ok) ok = writeConfig();
		getParams (args);
		receiveResult();
		getTime();
		if (logRequest) writeLogEntry();
		if (logDetails) writeHdrFile(header);
		checkHeader();
		uncompressData(streamType == 1);
		if (logDetails) writeDatFile();
	}

	private void getTime () {
		cal = new GregorianCalendar();
		logTime =
				cal.get(GregorianCalendar.YEAR)+"-"+
				String.format("%02d",(cal.get(GregorianCalendar.MONTH)+1))+"-"+
				String.format("%02d",cal.get(GregorianCalendar.DAY_OF_MONTH))+" "+
				String.format("%02d",cal.get(GregorianCalendar.HOUR_OF_DAY))+":"+
				String.format("%02d",cal.get(GregorianCalendar.MINUTE))+":"+
				String.format("%02d",cal.get(GregorianCalendar.SECOND));
		dmpTime = logTime.replace(':', '.').replace(' ', '_');
		dmpFileName = WSDir+"/log/DPL."+dmpTime;
	}
 
	private void getParams (String[] args) {
		portNr = Integer.parseInt(args[0]);
		timeout = Integer.parseInt(args[1]);
	}
 
	private void receiveResult () {
		try {
			serverSocket = new ServerSocket (portNr);
			socket = serverSocket.accept();
			InputStream is = socket.getInputStream();
			is.read(header, 0, headerSize);
			parseHeader();
			zipData = new byte[fileLength];
			is.read(zipData, 0, fileLength);
			is.close();
			socket.close();
			serverSocket.close();
		} catch (IOException e) {
			writeStackTrace(e);
		}
	}
	
	private void parseHeader () {
		fileLength = readHeader(0, fileLengthSize);
		int headIdx = fileLengthSize;
		checksum = new String (header, headIdx, MD5hashSize);
		headIdx += MD5hashSize;
		streamVersion = readHeader(headIdx, streamVersionSize);
		headIdx += streamVersionSize;
		streamType = readHeader(headIdx, streamTypeSize);
	}

	private int readHeader (int headIdx, int size) {
		String valueStr = new String(header, headIdx, size);
		return (Integer.parseInt(valueStr));
	}

	private void checkHeader () {
		String checksum2 = null;
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(zipData, 0, fileLength);
			byte[] hash = md.digest();
			checksum2 = new BigInteger(1, hash).toString(16);
			while (checksum2.length() < MD5hashSize) {
				checksum2 = "0"+checksum2;
			}
		} catch (NoSuchAlgorithmException e) {
			writeStackTrace(e);
		}
		if (checksum.compareTo(checksum2) != 0) {
			// Warn for checksum mismatch
		}
	}

	private void uncompressData (boolean compressed) {
		final int bufferSize = 8192;
		byte[] buffer = new byte[bufferSize];
		int bytesRead;
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(zipData, 0, fileLength);
			GZIPInputStream gzis = null;
			if (compressed) {
				gzis = new GZIPInputStream((InputStream)bais);
				bytesRead = gzis.read(buffer,  0,  bufferSize);
			} else
				bytesRead = bais.read(buffer,  0,  bufferSize);
			while (bytesRead > -1) {
				data.append(new String(buffer, 0, bytesRead));
				if (compressed)
					bytesRead = gzis.read(buffer,  0,  bufferSize);
				else
					bytesRead = bais.read(buffer,  0,  bufferSize);
			}
		} catch (IOException e) {
			writeStackTrace(e);
		}
	}

	private boolean readConfig() {
		try (
			FileInputStream fis = new FileInputStream(xmlFileName);
			BufferedInputStream bis = new BufferedInputStream(fis);
			XMLDecoder decoder = new XMLDecoder(bis);
			) {
			logRequest = (boolean)decoder.readObject();
			logDetails = (boolean)decoder.readObject();
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
			encoder.close();
			bos.close();
			fos.close();
			return (true);
		} catch (Exception e) {
			writeStackTrace(e);
			return (false);
		}
	}

	private void writeLogEntry () {
		try (
			FileWriter fw = new FileWriter(logFileName, true);
			BufferedWriter bw = new BufferedWriter(fw);
			PrintWriter logFile=new PrintWriter(bw);
			) {
    		logFile.println(String.format("%s %d", logTime, portNr));
            logFile.close();
            bw.close();
            fw.close();
		} catch (Exception e) {
			writeStackTrace(e);
		}
	}

	private void writeHdrFile (byte[] data) {
		try (
			PrintStream logFile = new PrintStream (dmpFileName+".hdr");
			) {
			logFile.write(data);
			logFile.close();
		} catch (IOException e) {
			writeStackTrace(e);
		}
	}

	private void writeDatFile () {
		try {
			PrintWriter txtFile = new PrintWriter (dmpFileName+".dat");
			txtFile.print(data);
			txtFile.close();
		} catch (FileNotFoundException e) {
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
