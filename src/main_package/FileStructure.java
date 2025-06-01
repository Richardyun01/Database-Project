package main_package;

import java.io.RandomAccessFile;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class FileStructure {
	private static final int BLOCK_SIZE = 1024;
	private static final int FIELD_NAME_SIZE = 16;

	// 필드 정보
	public static class FieldInfo {
	    String name;
	    int length;
	    public FieldInfo(String name, int length) {
	        this.name = name;
	        this.length = length;
	    }
	}

	// 메타데이터
	public static class Metadata {
	    int firstRecordBlock;
	    int firstRecordOffset;
	    int fieldCount;
	    List<FieldInfo> fields;

	    public Metadata(List<FieldInfo> fields) {
	        this.firstRecordBlock = -1;
	        this.firstRecordOffset = -1;
	        this.fieldCount = fields.size();
	        this.fields = fields;
	    }
	}

	// 레코드
	public static class Record {
	    int nextRecordBlock;
	    int nextRecordOffset;
	    List<String> fieldValues;
	    byte nullBitmap;

	    public Record(List<String> fieldValues, byte nullBitmap) {
	        this.nextRecordBlock = -1;
	        this.nextRecordOffset = -1;
	        this.fieldValues = fieldValues;
	        this.nullBitmap = nullBitmap;
	    }
	}

	// 메타데이터 헤더에 기록
	public void writeHeader(RandomAccessFile raf, Metadata meta) throws IOException {
	    ByteBuffer buffer = ByteBuffer.allocate(BLOCK_SIZE);
	    buffer.putInt(meta.firstRecordBlock);
	    buffer.putInt(meta.firstRecordOffset);
	    buffer.putInt(meta.fieldCount);

	    for (FieldInfo field : meta.fields) {
	        byte[] nameBytes = field.name.getBytes(StandardCharsets.UTF_8);
	        byte[] fixedName = new byte[FIELD_NAME_SIZE];
	        System.arraycopy(nameBytes, 0, fixedName, 0, Math.min(nameBytes.length, FIELD_NAME_SIZE));
	        buffer.put(fixedName);
	        buffer.putInt(field.length);
	    }

	    raf.seek(0);
	    raf.write(buffer.array());
	}

	// 헤더에서 메타데이터 읽기
	public Metadata readHeader(RandomAccessFile raf) throws IOException {
	    ByteBuffer buffer = ByteBuffer.allocate(BLOCK_SIZE);
	    raf.seek(0);
	    raf.readFully(buffer.array());
	    buffer.rewind();
	    int firstRecordBlock = buffer.getInt();
	    int firstRecordOffset = buffer.getInt();
	    int fieldCount = buffer.getInt();
	    List<FieldInfo> fields = new ArrayList<>();

	    for (int i = 0; i < fieldCount; i++) {
	        byte[] fixedName = new byte[FIELD_NAME_SIZE];
	        buffer.get(fixedName);
	        String name = new String(fixedName, StandardCharsets.UTF_8).trim();
	        int length = buffer.getInt();
	        fields.add(new FieldInfo(name, length));
	    }

	    Metadata meta = new Metadata(fields);
	    meta.firstRecordBlock = firstRecordBlock;
	    meta.firstRecordOffset = firstRecordOffset;
	    return meta;
	}
	
	// 레코드 크기 계산
	private int getRecordSize(Metadata meta) {
	    int size = 4 + 4 + 1; // nextBlock + nextOffset + nullBitmap
	    for (FieldInfo field : meta.fields) {
	        size += field.length;
	    }
	    return size;
	}

	// 레코드 삽입
	public void insertRecord(String filename, Metadata meta, Record newRecord) throws IOException {
	    try (RandomAccessFile raf = new RandomAccessFile(filename, "rw")) {
	        int recordSize = getRecordSize(meta);
	        int fileLength = (int) raf.length();
	        int lastBlockStart = (fileLength / BLOCK_SIZE) * BLOCK_SIZE;
	        int offsetInLastBlock = fileLength % BLOCK_SIZE;

	        int newBlock = fileLength / BLOCK_SIZE;
	        int newOffset = offsetInLastBlock;

	        // 새 블록으로 이동
	        if (offsetInLastBlock + recordSize > BLOCK_SIZE) {
	            newBlock++;
	            newOffset = 0;
	        }

	        // 헤더에 위치 기록
	        if (meta.firstRecordBlock == -1 && meta.firstRecordOffset == -1) {
	            meta.firstRecordBlock = newBlock;
	            meta.firstRecordOffset = newOffset;
	            raf.seek(0);
	            raf.writeInt(meta.firstRecordBlock);
	            raf.writeInt(meta.firstRecordOffset);
	        } else {
	        	// 새로운 레코드로 연결
	            int currBlock = meta.firstRecordBlock;
	            int currOffset = meta.firstRecordOffset;

	            while (true) {
	                raf.seek((long) currBlock * BLOCK_SIZE + currOffset);
	                int nextBlock = raf.readInt();
	                int nextOffset = raf.readInt();

	                if (nextBlock == -1 && nextOffset == -1) {
	                    raf.seek((long) currBlock * BLOCK_SIZE + currOffset);
	                    raf.writeInt(newBlock);
	                    raf.writeInt(newOffset);
	                    break;
	                } else {
	                    currBlock = nextBlock;
	                    currOffset = nextOffset;
	                }
	            }
	        }

	        writeRecord(raf, newBlock, newOffset, meta, newRecord);
	    }
	}

	// 레코드 위치 기록
	public void writeRecord(RandomAccessFile raf, int block, int offset, Metadata meta, Record record) throws IOException {
	    int recordSize = getRecordSize(meta);
	    raf.seek((long) block * BLOCK_SIZE + offset);
	    ByteBuffer buffer = ByteBuffer.allocate(recordSize);

	    buffer.putInt(record.nextRecordBlock);
	    buffer.putInt(record.nextRecordOffset);
	    buffer.put(record.nullBitmap);

	    for (int i = 0; i < meta.fieldCount; i++) {
	    	    byte[] fixedField = new byte[meta.fields.get(i).length];
	    	    Arrays.fill(fixedField, (byte) ' ');
	    	
	    	    // null 비트가 0이면 실제 값 복사, 1이면 공백 그대로 남김
	    	    if (((record.nullBitmap >> i) & 1) == 0) {
	    	        String val = record.fieldValues.get(i);
	    	        byte[] src = val.getBytes(StandardCharsets.UTF_8);
	    	        System.arraycopy(src, 0, fixedField, 0,
	    	                         Math.min(src.length, fixedField.length));
	    	    }
	    	    buffer.put(fixedField); 
	    }

	    raf.write(buffer.array());
	}
	
	public Record readRecord(RandomAccessFile raf, Metadata meta, int block, int offset) throws IOException {
	    raf.seek((long)block * BLOCK_SIZE + offset);
	    int nextBlock = raf.readInt();
	    int nextOffset= raf.readInt();
	    byte nullBitmap = raf.readByte();
	    List<String> vals = new ArrayList<>(meta.fieldCount);
	    for (FieldInfo f : meta.fields) {
	        byte[] buf = new byte[f.length];
	        raf.readFully(buf);
	        vals.add(new String(buf, StandardCharsets.UTF_8).trim());
	    }
	    Record r = new Record(vals, nullBitmap);
	    r.nextRecordBlock  = nextBlock;
	    r.nextRecordOffset = nextOffset;
	    return r;
	}
}
