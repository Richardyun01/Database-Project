package main_package;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
// import java.util.*;

public class RecordSearcher {
	private static final int BLOCK_SIZE = 1024;
	private static final int FIELD_NAME_SIZE = 16;

	// 헤더에서 메타데이터 읽기
	public FileStructure.Metadata readHeader(RandomAccessFile raf) throws IOException {
	    ByteBuffer buffer = ByteBuffer.allocate(BLOCK_SIZE);
	    raf.seek(0);
	    raf.readFully(buffer.array());
	    buffer.rewind();
	    int firstRecordBlock = buffer.getInt();
	    int firstRecordOffset = buffer.getInt();
	    int fieldCount = buffer.getInt();

	    FileStructure.Metadata meta = new FileStructure.Metadata(new java.util.ArrayList<>());
	    meta.firstRecordBlock = firstRecordBlock;
	    meta.firstRecordOffset = firstRecordOffset;
	    meta.fieldCount = fieldCount;

	    for (int i = 0; i < fieldCount; i++) {
	        byte[] fixedName = new byte[FIELD_NAME_SIZE];
	        buffer.get(fixedName);
	        String name = new String(fixedName, StandardCharsets.UTF_8).trim();
	        int length = buffer.getInt();
	        meta.fields.add(new FileStructure.FieldInfo(name, length));
	    }
	    return meta;
	}

	// 탐색키 범위 레코드 검색
	public void searchRecordsByKeyRange(String fileName, String minKey, String maxKey) {
	    String dataFilename = fileName + ".dat";
	    try (RandomAccessFile raf = new RandomAccessFile(dataFilename, "r")) {
	        FileStructure.Metadata meta = readHeader(raf);
	        int keyLength = meta.fields.get(0).length;

	        int block = meta.firstRecordBlock;
	        int offset = meta.firstRecordOffset;

	        while (block != -1 && offset != -1) {
	            raf.seek((long) block * BLOCK_SIZE + offset);
	            int nextRecordBlock = raf.readInt();
	            int nextRecordOffset = raf.readInt();
	            byte nullBitmap = raf.readByte();

	            byte[] keyBytes = new byte[keyLength];
	            raf.read(keyBytes);
	            String key = new String(keyBytes, StandardCharsets.UTF_8).trim();
	            // System.out.println("keylength = " + key.length());

	            // 키가 범위 내에 있으면 출력
	            if (key.compareTo(minKey) >= 0 && key.compareTo(maxKey) <= 0) {
	                System.out.println("Block:" + block + " Offset:" + offset + " Search-key:" + key);

	                int dataOffset = 9 + keyLength; // 포인터+비트맵+길이
	                for (int i = 1; i < meta.fieldCount; i++) { // 0번 키 제외
	                    if (((nullBitmap >> i) & 1) == 0) {
	                        raf.seek((long) block * BLOCK_SIZE + offset + dataOffset);
	                        byte[] fieldBytes = new byte[meta.fields.get(i).length];
	                        raf.read(fieldBytes);
	                        String fieldValue = new String(fieldBytes, StandardCharsets.UTF_8).trim();
	                        System.out.println(meta.fields.get(i).name + ": " + fieldValue);
	                        dataOffset += meta.fields.get(i).length;
	                    } else {
	                        System.out.println(meta.fields.get(i).name + ": null");
	                    }
	                }
	            }

	            block = nextRecordBlock;
	            offset = nextRecordOffset;
	        }

	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	}

	/*
	public static void main(String[] args) {
	    RecordSearcher searcher = new RecordSearcher();
	    searcher.searchRecordsByKeyRange("f1", "0001", "9999");
	}
	*/
}
