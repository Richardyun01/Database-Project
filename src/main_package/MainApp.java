package main_package;

import java.util.*;
import java.io.*;

public class MainApp {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        FileStructure fileStructure = new FileStructure();
        RecordBulkInserter inserter = new RecordBulkInserter();
        FieldSearcher fieldSearcher = new FieldSearcher();
        RecordSearcher recordSearcher = new RecordSearcher();
        MetadataManager metadataManager = new MetadataManager();

        while (true) {
            System.out.println("\n==== Database System ====");
            System.out.println("1. Create new data file (from config .txt)");
            System.out.println("2. Record Insertion (With data file)");
            System.out.println("3. Search Field");
            System.out.println("4. Search Record Range");
            System.out.println("5. Check Metadata(Memory)");
            System.out.println("6. Exit");
            System.out.println("7. SQL Merge Process");
            System.out.println("8. Validate SQL JOIN vs Merge-Join");
            System.out.print("Selection: ");
            String choice = scanner.nextLine();

            switch (choice) {
                case "1":
                    System.out.print("Enter config text file path to create .dat: ");
                    String configPath = scanner.nextLine().trim();
                    boolean createSuccess = createNewDataFileFromConfig(configPath, fileStructure, metadataManager);
                    if (!createSuccess) {
                        System.out.println("Failed to create data file due to invalid config.");
                    }
                    break;

                case "2":
                    System.out.print("Enter file name to insert into (without extension): ");
                    String insertFileName = scanner.nextLine().trim();
                    System.out.print("Record insertion data file path: ");
                    String dataFilePath = scanner.nextLine();
                    inserter.bulkInsertFromDataFile(insertFileName, dataFilePath);
                    break;

                case "3":
                    System.out.print("File name: ");
                    String fNameField = scanner.nextLine();
                    System.out.print("Search field name: ");
                    String field = scanner.nextLine();
                    fieldSearcher.searchField(fNameField, field);
                    break;

                case "4":
                    System.out.print("File name: ");
                    String fNameRange = scanner.nextLine();
                    System.out.print("Search-key minimum value: ");
                    String minKey = scanner.nextLine();
                    System.out.print("Search-key maximum value: ");
                    String maxKey = scanner.nextLine();
                    recordSearcher.searchRecordsByKeyRange(fNameRange, minKey, maxKey);
                    break;

                case "5":
                    System.out.print("Enter file name (without extension) to check metadata: ");
                    String metaFileName = scanner.nextLine().trim();
                    metadataManager.loadMetadataFromFile(metaFileName);

                    List<FileStructure.FieldInfo> loadedFields = metadataManager.getFieldsInMemory();
                    if (loadedFields.isEmpty()) {
                        System.out.println("No metadata loaded or file not found.");
                    } else {
                        System.out.println("List of fields in memory for file '" + metaFileName + ".dat':");
                        for (int i = 0; i < loadedFields.size(); i++) {
                            System.out.println("  " + i + ": " 
                                + loadedFields.get(i).name + " (length=" + loadedFields.get(i).length + ")");
                        }
                    }
                    break;

                case "6":
                    System.out.println("Program exited.");
                    scanner.close();
                    return;
                    
                case "7":
                    System.out.print("Enter first table file name: ");
                    String tableR = scanner.nextLine().trim();
                    System.out.print("Enter second table file name: ");
                    String tableS = scanner.nextLine().trim();
                    System.out.print("Enter join key column name: ");
                    String joinKey = scanner.nextLine().trim();
                    System.out.print("Enter output file name (without .dat): ");
                    String outName = scanner.nextLine().trim();

                    JoinProcessor jp = new JoinProcessor();
                    try {
                        jp.executeMergeJoin(tableR, tableS, joinKey, outName);
                    } catch (Exception e) {
                        System.out.println("Failed to run merge join: " + e.getMessage());
                    }
                    break;
                    
                case "8":
                	System.out.print("Enter full SQL JOIN query: ");
                	String userSql = scanner.nextLine().trim();

                	System.out.print("Enter first table name (for merge algorithm): ");
                	String tblR = scanner.nextLine().trim();
                	System.out.print("Enter second table name: ");
                	String tblS = scanner.nextLine().trim();
                	System.out.print("Enter join key column name: ");
                	String jk   = scanner.nextLine().trim();

                	JoinProcessor validator = new JoinProcessor();
                	try {
                	    validator.validateWithSqlJoin(userSql, tblR, tblS, jk);
                	} catch (Exception e) {
                	    System.out.println("Validation failed: " + e.getMessage());
                	}
                	break;

                default:
                    System.out.println("Wrong input.");
                    break;
            }
        }
    }

    // 필드 정보 입력
    private static boolean createNewDataFileFromConfig(String configPath, FileStructure fileStructure, MetadataManager metadataManager) {
        try (BufferedReader br = new BufferedReader(new FileReader(configPath))) {
            String line = br.readLine();
            if (line == null || line.trim().isEmpty()) {
                System.out.println("Config file empty or invalid.");
                return false;
            }
            line = line.trim();
            String[] parts = line.split(";");
            if (parts.length < 3) {
                System.out.println("Not enough fields in config. Format must be: fileName;fieldCount;fieldName...;fieldLength...");
                return false;
            }

            String fileName = parts[0].trim();
            int fieldCount = Integer.parseInt(parts[1].trim());

            int totalNeeded = 2 + fieldCount + fieldCount;  // "파일명,필드개수" + "필드Count" + "길이Count"
            if (parts.length != totalNeeded) {
                System.out.println("Mismatched field count vs actual input. Needed " + totalNeeded + " parts, got " + parts.length);
                return false;
            }

            List<String> fieldNames = new ArrayList<>();
            int idx = 2;
            for (int i = 0; i < fieldCount; i++) {
                fieldNames.add(parts[idx++].trim());
            }

            List<Integer> fieldLengths = new ArrayList<>();
            for (int i = 0; i < fieldCount; i++) {
                fieldLengths.add(Integer.parseInt(parts[idx++].trim()));
            }

            List<FileStructure.FieldInfo> fields = new ArrayList<>();
            for (int i = 0; i < fieldCount; i++) {
                fields.add(new FileStructure.FieldInfo(fieldNames.get(i), fieldLengths.get(i)));
            }

            String dataFile = fileName + ".dat";
            try (RandomAccessFile raf = new RandomAccessFile(dataFile, "rw")) {
                fileStructure.writeHeader(raf, new FileStructure.Metadata(fields));
                System.out.println("File '" + dataFile + "' created with user-defined fields from " + configPath);
                metadataManager.loadMetadataFromFile(fileName);
            } catch (Exception e) {
                System.out.println("Error creating file: " + e.getMessage());
                return false;
            }

            return true;

        } catch (IOException e) {
            System.out.println("Could not read config file: " + e.getMessage());
            return false;
        } catch (NumberFormatException e) {
            System.out.println("Invalid integer value in config: " + e.getMessage());
            return false;
        }
    }
}
