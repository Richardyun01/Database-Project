package main_package;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

import main_package.MetadataManager;

public class RecordBulkInserter {
	// 조인 테스트용 인자
	private static final boolean ALLOW_DUPLICATE_KEYS = false;
	
	private FileStructure fileStructure = new FileStructure();
	private RecordSearcher recordSearcher = new RecordSearcher();
	private MetadataManager metadataManager = new MetadataManager();

	public void bulkInsertFromDataFile(String fileName, String dataFilePath) {
	    try (BufferedReader br = new BufferedReader(new FileReader(dataFilePath))) {
	        String dataFilename = fileName + ".dat";

	        FileStructure.Metadata meta;
	        try (RandomAccessFile raf = new RandomAccessFile(dataFilename, "rw")) {
	            meta = fileStructure.readHeader(raf);
	        } catch (IOException e) {
	            System.out.println("Failed to read header from file: " + fileName);
	            return;
	        }

	        List<FileStructure.Record> recordList = new ArrayList<>();
	        List<String> searchKeys = new ArrayList<>();

	        String line;
	        while ((line = br.readLine()) != null) {
	            line = line.trim();
	            if (line.isEmpty()) continue;

	            String[] parts = line.split(",", 2);
	            if (parts.length < 2) {
	                System.out.println("Invalid record line: " + line);
	                continue;
	            }

	            String recordsData = parts[1].trim();
	            String[] recordStrArr = recordsData.split("\\|");
	            for (String recStr : recordStrArr) {
	                String[] fieldValuesArr = recStr.split(";");
	                List<String> fieldValues = new ArrayList<>();
	                byte nullBitmap = 0;

	                for (int i = 0; i < fieldValuesArr.length; i++) {
	                    String val = fieldValuesArr[i].trim();
	                    if (val.isEmpty() || val.equalsIgnoreCase("null")) {
	                        nullBitmap |= (1 << i);
	                        fieldValues.add("");
	                    } else {
	                        fieldValues.add(val);
	                    }
	                }

	                String searchKey = fieldValues.get(0);
	                if (!searchKey.isEmpty()) {
	                    searchKeys.add(searchKey);
	                }

	                FileStructure.Record record = new FileStructure.Record(fieldValues, nullBitmap);
	                recordList.add(record);
	            }
	        }

	        recordList.sort((r1, r2) -> r1.fieldValues.get(0).compareTo(r2.fieldValues.get(0)));

	        for (int i = 0; i < recordList.size() - 1; i++) {
	            String currKey = recordList.get(i).fieldValues.get(0);
	            String nextKey = recordList.get(i + 1).fieldValues.get(0);
	            if (currKey.equals(nextKey) && !ALLOW_DUPLICATE_KEYS) {
	                System.out.println("Insertion failed due to duplicated keys in record file.");
	                return;
	            }
	        }

	        for (FileStructure.Record rec : recordList) {
	            fileStructure.insertRecord(dataFilename, meta, rec);
	        }

	        System.out.println("Records inserted into file '" + dataFilename + "' successfully (no duplicate keys).");

	        metadataManager.createTableInMySQL(fileName, meta);
	        insertAllRecordsIntoMySQL(fileName, meta, recordList);

	        if (!searchKeys.isEmpty()) {
	            Collections.sort(searchKeys);
	            String minKey = searchKeys.get(0);
	            String maxKey = searchKeys.get(searchKeys.size() - 1);
	            System.out.println("Auto search: range " + minKey + " ~ " + maxKey);
	            recordSearcher.searchRecordsByKeyRange(fileName, minKey, maxKey);
	        }

	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	}
	
	private void insertAllRecordsIntoMySQL(String tableName, FileStructure.Metadata meta, List<FileStructure.Record> records) {
	    String url = "jdbc:mysql://localhost:3306/mydb";
	    String user = "root";
	    String pass = "1234";

	    try (Connection conn = DriverManager.getConnection(url, user, pass)) {
	        StringBuilder sb = new StringBuilder();
	        sb.append("INSERT INTO ").append(tableName).append(" (");
	        for (int i = 0; i < meta.fieldCount; i++) {
	            sb.append(meta.fields.get(i).name);
	            if (i < meta.fieldCount - 1) sb.append(",");
	        }
	        sb.append(") VALUES(");
	        for (int i = 0; i < meta.fieldCount; i++) {
	            sb.append("?");
	            if (i < meta.fieldCount - 1) sb.append(",");
	        }
	        sb.append(")");

	        String insertSQL = sb.toString();
	        System.out.println("Insert SQL: " + insertSQL);

	        try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
	            for (FileStructure.Record rec : records) {
	                for (int i = 0; i < meta.fieldCount; i++) {
	                    String val = rec.fieldValues.get(i);
	                    if (((rec.nullBitmap >> i) & 1) == 1) {
	                        pstmt.setNull(i+1, Types.CHAR); 
	                    } else {
	                        pstmt.setString(i+1, val);
	                    }
	                }
	                pstmt.addBatch();
	            }
	            pstmt.executeBatch();
	        }
	        System.out.println("All records inserted into MySQL table '" + tableName + "'.");

	    } catch (SQLException e) {
	        e.printStackTrace();
	    }
	}

	/*
	public static void main(String[] args) {
	    String dataFilePath = "D:\SchoolHomework\4-1\Database\testdata.txt";
	    RecordBulkInserter inserter = new RecordBulkInserter();
	    inserter.bulkInsertFromDataFile(dataFilePath);
	}
	*/
}
