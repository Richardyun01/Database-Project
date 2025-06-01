package main_package;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class MetadataManager {
    private static final int BLOCK_SIZE = 1024;
    private static final int FIELD_NAME_SIZE = 16;

    // 필드 리스트
    private List<FileStructure.FieldInfo> fieldsInMemory = new ArrayList<>();

    public void loadMetadataFromFile(String fileName) {
        String dataFile = fileName + ".dat";
        try (RandomAccessFile raf = new RandomAccessFile(dataFile, "r")) {
            ByteBuffer buffer = ByteBuffer.allocate(BLOCK_SIZE);
            raf.seek(0);
            raf.readFully(buffer.array());
            buffer.rewind();

            int firstRecordBlock = buffer.getInt();		// 첫 레코드 블록 번호
            int firstRecordOffset = buffer.getInt();	// 첫 레코드 오프셋
            int fieldCount = buffer.getInt(); 			// 필드 개수

            List<FileStructure.FieldInfo> tempFields = new ArrayList<>();
            for (int i = 0; i < fieldCount; i++) {
                byte[] fixedName = new byte[FIELD_NAME_SIZE];
                buffer.get(fixedName);
                String name = new String(fixedName, StandardCharsets.UTF_8).trim();
                int length = buffer.getInt();
                tempFields.add(new FileStructure.FieldInfo(name, length));
            }

            fieldsInMemory = tempFields; // 메모리 구조에 저장
            System.out.println("Metadata is loaded to memory from File " + fileName + ".dat");
            System.out.println("Field numbers: " + fieldCount);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 필드 정보 반환
    public List<FileStructure.FieldInfo> getFieldsInMemory() {
        return fieldsInMemory;
    }

    // 파일 헤더에 저장
    public void saveMetadataToFile(String fileName, int firstRecordBlock, int firstRecordOffset) {
        String dataFile = fileName + ".dat";
        try (RandomAccessFile raf = new RandomAccessFile(dataFile, "rw")) {
            ByteBuffer buffer = ByteBuffer.allocate(BLOCK_SIZE);

            buffer.putInt(firstRecordBlock);
            buffer.putInt(firstRecordOffset);
            buffer.putInt(fieldsInMemory.size());

            for (FileStructure.FieldInfo field : fieldsInMemory) {
                byte[] nameBytes = field.name.getBytes(StandardCharsets.UTF_8);
                byte[] fixedName = new byte[FIELD_NAME_SIZE];
                System.arraycopy(nameBytes, 0, fixedName, 0, Math.min(nameBytes.length, FIELD_NAME_SIZE));
                buffer.put(fixedName);
                buffer.putInt(field.length);
            }

            raf.seek(0);
            raf.write(buffer.array());
            System.out.println("File " + fileName + ".dat's header information has been updated.");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public void createTableInMySQL(String tableName, FileStructure.Metadata meta) {
        String url = "jdbc:mysql://localhost:3306/mydb";
        String user = "root";
        String pass = "1234";

        try (Connection conn = DriverManager.getConnection(url, user, pass);
             Statement stmt = conn.createStatement()) {
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");
            for (int i = 0; i < meta.fieldCount; i++) {
                String colName = meta.fields.get(i).name;
                int colLen = meta.fields.get(i).length;
                sb.append(colName).append(" CHAR(").append(colLen).append(")");
                if (i < meta.fieldCount - 1) sb.append(", ");
            }
            sb.append(")");

            stmt.executeUpdate(sb.toString());
            System.out.println("MySQL table '" + tableName + "' created/verified.");
        } catch (SQLException e) {
        	System.out.println("[ERROR] Failed to create MySQL table '" + tableName + "'.");
            System.out.println("Reason: " + e.getMessage());
            e.printStackTrace();
        }
    }
}