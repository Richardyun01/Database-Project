package main_package;

import java.io.*;

public class FieldSearcher {
	private final FileStructure fs = new FileStructure();

	// 모든 레코드 값 검색
	public void searchField(String fileName, String searchField) {
	    String dataFilename = fileName + ".dat";
	    try (RandomAccessFile raf = new RandomAccessFile(dataFilename, "r")) {
	        FileStructure.Metadata meta = fs.readHeader(raf);

	        // 인덱스 검색
	        int fieldIndex = -1;
	        for (int i = 0; i < meta.fieldCount; i++) {
	            if (meta.fields.get(i).name.equalsIgnoreCase(searchField)) {
	                fieldIndex = i;
	                break;
	            }
	        }
	        if (fieldIndex == -1) {
	            System.out.println("Cannot find field '" + searchField + "'.");
	            return;
	        }

	        int block  = meta.firstRecordBlock;
            int offset = meta.firstRecordOffset;
            while (block != -1 && offset != -1) {
	            FileStructure.Record rec = fs.readRecord(raf, meta, block, offset);
	            String val = rec.fieldValues.get(fieldIndex).trim();
	            System.out.println(
	                "Block:" + block +
	                " Offset:" + offset +
	                " Field " + searchField +
	                " Value: " + (val.isEmpty() ? "null" : val)
	            );
	
	            block  = rec.nextRecordBlock;
	            offset = rec.nextRecordOffset;
	        }

	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	}

	/*
	public static void main(String[] args) {
	    FieldSearcher searcher = new FieldSearcher();
	    searcher.searchField("f1", "C");
	}
	*/
}
