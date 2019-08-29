package se.lu.canmove.ws;

import java.beans.XMLDecoder;
import java.beans.XMLEncoder;
import java.io.*;
import java.util.GregorianCalendar;
import java.sql.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.GZIPOutputStream;
import java.net.*;

public class DataProviderSender {

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
	private String xmlFileName = WSDir+"/DataProviderSender.xml";
	private String logFileName = WSDir+"/DataProviderSender.log";
	private String dmpFileName = WSDir+"/log/DPS."+dmpTime;
	private boolean logRequest = false;
	private boolean logDetails = false;
	
	private String[] query;
	private int maxrow = Integer.MAX_VALUE;
	private String userId;
	private int portNr;
	private int timeout = 0;

    private String HostName;
    private String IPHostName = "localhost";
    private InetAddress IPAddress;
    private Socket socket;
	private String DBDriver = "org.postgresql.Driver";
	private String DBString = "jdbc:postgresql:canmove";
	private String DBUser = "canmove";
	private String DBPassword = "";

	private String sql;
	private String targetView;
	private int ecode = 0;
	private String emsg;
	
    private final int fileLengthSize = 16;
    private final int MD5hashSize = 32;
    private final int streamVersionSize = 4;
    private final int streamTypeSize = 4;
    private final int headerSize = MD5hashSize + fileLengthSize + streamVersionSize + streamTypeSize; 
    private final int streamVersion = 1;
    private int streamType = 1;

    private byte[] header = new byte[headerSize];
    private byte[] data = null;
    private byte[] zipData = null;

	public static void main(String[] args) {
		DataProviderSender DPS = new DataProviderSender();
		DPS.run (args);
	}

	public void run(String[] args) {
		boolean ok = readConfig();
		if (!ok) ok = writeConfig();
		getParams (args);
		
		if (ecode == 0) {
			runQuery();
			compressData();
			createHeader(zipData);
			sendResult(zipData);
		}
		// 2019-08-28-Mathieu: change to adapt the result in case of bad collection. Return an empty file, no error thrown
		else if (ecode == 101) {
			
			// creata empty data
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			baos.write("\n".getBytes(), 0,  1);
			data = baos.toByteArray();
			if (logDetails) writeDmpFile (data, "dat");
			
			compressData();
			createHeader(zipData);
			sendResult(zipData);
		}
		else {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			baos.write(emsg.getBytes(), 0,  emsg.length());
			data = baos.toByteArray();
			streamType = ecode;
			if (logDetails) writeDmpFile (data, "dat");
			createHeader(data);
			sendResult(data);
		}
	}

	private void getParams (String[] args) {
	    try {
			HostName = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			writeStackTrace(e);
		}

		query = args[0].split("/");
		if ((query[0]!=null) && (query[0].compareTo("")!=0))
			maxrow = Integer.parseInt(query[0]);
		if (maxrow < 0) {
			maxrow = -maxrow;
			IPHostName = "localhost";
		}

		translateTarget (query[2]);
		
		sql = "select ";
		if ((query[1]!=null) && (query[1].compareTo("")!=0))
			sql += query[1];
		else
			sql += "*";
		sql += " from "+targetView;
		if ((query.length>3) && (query[3]!=null) && (query[3].compareTo("")!=0))
			sql += " where "+query[3];
		if ((query.length>4) && (query[4]!=null) && (query[4].compareTo("")!=0))
			sql += " order by "+query[4];
		userId = args[1];
		portNr = Integer.parseInt(args[2]);
		timeout = Integer.parseInt(args[3]);

		if (logRequest) writeLogEntry(query[2]);
		if (logDetails) writeParFile(args);
	}
 
	private void translateTarget (String targetCollection) {
		try {
			Class.forName(DBDriver);
			Connection db = DriverManager.getConnection(DBString, DBUser, DBPassword);
			Statement st = db.createStatement();
			String sql = "select int_object from iw_table where ext_object = '"+
						targetCollection.toLowerCase()+"' and active"; 
			ResultSet rs = st.executeQuery(sql);
			Object obj = null;
			if (rs.next() && (!rs.wasNull())) {
				obj = rs.getObject(1);
				targetView = ((String)obj).toString();
				ecode = 0;
			} else {
				targetView = "<invalid collection>";
				ecode = 101;
				emsg = "Invalid collection: "+targetCollection;
			}
			rs.close();
			st.close();
			db.close();
		} catch (ClassNotFoundException | SQLException e) {
			writeStackTrace(e);
			ecode = 111;
			emsg = e.getMessage();
		} 
	}

	private void runQuery () {
		try {
			Class.forName(DBDriver);
			Connection db = DriverManager.getConnection(DBString, DBUser, DBPassword);
			Statement st = db.createStatement();
			ResultSet rs = st.executeQuery(sql);
			ResultSetMetaData rsmd = rs.getMetaData();
			int colnum = rsmd.getColumnCount();
			String[] coltype = new String[colnum+1];
			String colvalue = null;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			int i;
			
			for (i=1; i<colnum+1; i++) {
				coltype[i] = rsmd.getColumnTypeName(i);
			}

			Object obj = null;
			int rownum = 0;
			while (rs.next()) {
				for (i=1; i<=colnum; i++) {
					obj = rs.getObject(i);
					if (!rs.wasNull()) {
						switch (coltype[i]) {
						case "bool":
							colvalue = ((Boolean)obj).toString(); break;
						case "date":
							colvalue = ((Date)obj).toString(); break;
						case "float8":
						case "numeric":
							colvalue = ((Double)obj).toString(); break;
						case "int4":
						case "serial":
							colvalue = ((Integer)obj).toString(); break;
						case "text":
						case "varchar":
							colvalue = ((String)obj).toString(); break;
						case "time":
							colvalue = ((Time)obj).toString(); break;
						case "timestamp":
							colvalue = ((Timestamp)obj).toString(); break;
						default:
							colvalue = "unknown type("+coltype[i]+")"; break;
						}
						//baos.write(colvalue.getBytes(), 0,  colvalue.length());
						baos.write(colvalue.getBytes());
					}
					if (i<colnum)
						baos.write(",".getBytes(), 0,  1);
				}
				baos.write("\n".getBytes(), 0,  1);
				if (++rownum>=maxrow) break;
			}
			data = baos.toByteArray();
			rs.close();
			st.close();
			db.close();
		} catch (ClassNotFoundException | IOException | SQLException e) {
			writeStackTrace(e);
			ecode = 111;
			emsg = e.getMessage();
		}
		if (logDetails) writeDmpFile (data, "dat");
	}

	private void compressData () {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			GZIPOutputStream gzos = new GZIPOutputStream(baos);

			gzos.write(data,  0,  data.length);
			gzos.finish();
			zipData = baos.toByteArray();
		} catch (IOException e) {
			writeStackTrace(e);
		}
		if (logDetails) writeDmpFile (zipData, "zip");
	}

	private void createHeader (byte[] data) {
		String checksum = null;
		int headIdx = 0;
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(data, 0, data.length);
			byte[] hash = md.digest();
			checksum = new BigInteger(1, hash).toString(16);
			while (checksum.length() < MD5hashSize) {
				checksum = "0"+checksum;
			}
		} catch (NoSuchAlgorithmException e) {
			writeStackTrace(e);
		}
		headIdx = writeHeaderItem(0, fileLengthSize, data.length);
		for (int i=0; headIdx<(fileLengthSize+MD5hashSize); i++) {
			header[headIdx] = (byte)checksum.charAt(i);
			headIdx++;
		}
		headIdx = writeHeaderItem(headIdx, streamVersionSize, streamVersion);
		headIdx = writeHeaderItem(headIdx, streamTypeSize, streamType);
	}

	private int writeHeaderItem (int headIdx, int size, int value) {
		String formatStr = null;
		String valueStr = null;

		formatStr = String.format("%%0%dd", size);
		valueStr = String.format(formatStr, value);
		for (int i=0; i<size; i++) {
			header[headIdx] = (byte)valueStr.charAt(i);
			headIdx++;
		}
		return (headIdx);
	}

	private void sendResult (byte[] data) {
		if (logDetails) writeDmpFile (header, "hdr");
		try {
			IPAddress = InetAddress.getByName(IPHostName);
			socket = new Socket (IPAddress, portNr);
			OutputStream os = socket.getOutputStream();
			os.write(header, 0, headerSize);
			os.write(data, 0, data.length);
			os.close();
			socket.close();
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
			IPHostName = (String)decoder.readObject();
			DBDriver = (String)decoder.readObject();
			DBString = (String)decoder.readObject();
			DBUser = (String)decoder.readObject();
			DBPassword = (String)decoder.readObject();
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
			encoder.writeObject(IPHostName);
			encoder.writeObject(DBDriver);
			encoder.writeObject(DBString);
			encoder.writeObject(DBUser);
			encoder.writeObject(DBPassword);
			encoder.close();
			bos.close();
			fos.close();
			return (true);
		} catch (Exception e) {
			writeStackTrace(e);
			return (false);
		}
	}

	private void writeLogEntry (String target) {
		try (
			FileWriter fw = new FileWriter(logFileName, true);
			BufferedWriter bw = new BufferedWriter(fw);
			PrintWriter logFile=new PrintWriter(bw);
			) {
    		logFile.println(logTime+" "+userId+" "+target);
            logFile.close();
            bw.close();
            fw.close();
		} catch (Exception e) {
			writeStackTrace(e);
		}
	}

	private void writeParFile (String[] args) {
		try (
			PrintWriter parFile = new PrintWriter (dmpFileName+".par");
			) {
			parFile.println("----");
			for (int i=0; i<args.length; i++)
				parFile.println(args[i]);
			parFile.println("----");
			for (int i=0; i<query.length; i++)
				parFile.println(String.format("%d: ",i)+"/"+query[i]+"/");
			parFile.println("----");
			parFile.println("SQL: "+sql);
			parFile.println("----");
			parFile.println(String.format("query: %d", query.length));
			parFile.println(String.format("maxrow: %d", maxrow));
			parFile.println("userId: "+userId);
			parFile.println(String.format("portNr: %d", portNr));
			parFile.println(String.format("timeout: %d", timeout));
			parFile.println("HostName: "+HostName);
			parFile.println("IPHostName: "+IPHostName);
			parFile.close();
		} catch (FileNotFoundException e) {
			writeStackTrace(e);
		}
	}

	private void writeDmpFile (byte[] data, String suffix) {
		try (
			PrintStream logFile = new PrintStream (dmpFileName+"."+suffix);
			) {
			logFile.write(data);
			logFile.close();
		} catch (IOException e) {
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
