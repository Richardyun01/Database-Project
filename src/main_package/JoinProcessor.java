package main_package;

import java.io.RandomAccessFile;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class JoinProcessor {
    private static final String URL  = "jdbc:mysql://localhost:3306/mydb";
    private static final String USER = "root";
    private static final String PASS = "1234";

    private final FileStructure fileStructure = new FileStructure();

    // 머지 조인 알고리즘
    private List<List<String>> performMergeJoinRows(Connection conn, String tableR, String tableS, String joinKey) throws SQLException {
        List<List<String>> result = new ArrayList<>();

        try (
            Statement stR = conn.createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            Statement stS = conn.createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            ResultSet rsR = stR.executeQuery(
                "SELECT * FROM " + tableR + " ORDER BY " + joinKey);
            ResultSet rsS = stS.executeQuery(
                "SELECT * FROM " + tableS + " ORDER BY " + joinKey)
        ) {
            ResultSetMetaData mR = rsR.getMetaData(), mS = rsS.getMetaData();
            int cR = mR.getColumnCount(), cS = mS.getColumnCount();
            int idxR = -1, idxS = -1;
            for (int i = 1; i <= cR; i++) {
                if (mR.getColumnName(i).equalsIgnoreCase(joinKey)) idxR = i;
            }
            for (int i = 1; i <= cS; i++) {
                if (mS.getColumnName(i).equalsIgnoreCase(joinKey)) idxS = i;
            }
            if (idxR < 0 || idxS < 0) {
                throw new SQLException("Join key not found in both tables");
            }

            boolean hasR = rsR.next(), hasS = rsS.next();
            while (hasR && hasS) {
                String keyR = rsR.getString(idxR), keyS = rsS.getString(idxS);
                
                // NULL은 매칭 대상에서 제외
                if (keyR == null) { hasR = rsR.next(); continue; }
                if (keyS == null) { hasS = rsS.next(); continue; }

                int cmp = keyR.compareTo(keyS);
                if (cmp < 0) {
                    hasR = rsR.next();
                } else if (cmp > 0) {
                    hasS = rsS.next();
                } else {
                    // 같은 키 그룹 버퍼링
                    List<List<String>> bufR = new ArrayList<>();
                    List<List<String>> bufS = new ArrayList<>();
                    String cur = keyR;
                    do {
                        bufR.add(readRow(rsR, cR));
                        hasR = rsR.next();
                    } while (hasR && cur.equals(rsR.getString(idxR)));
                    do {
                        bufS.add(readRow(rsS, cS));
                        hasS = rsS.next();
                    } while (hasS && cur.equals(rsS.getString(idxS)));

                    // 조인
                    for (List<String> r : bufR) {
                        for (List<String> s : bufS) {
                            List<String> merged = new ArrayList<>(r);
                            for (int j = 0; j < s.size(); j++) {
                            	merged.add(s.get(j));
                            }
                            result.add(merged);
                        }
                    }
                }
            }
        }

        return result;
    }

    // 파일에 저장
    public void executeMergeJoin(String tableR, String tableS, String joinKey, String outTable) throws Exception {
        String datFile = outTable + ".dat";
        Set<String> seenMerged = new HashSet<>();

        try (Connection conn = DriverManager.getConnection(URL, USER, PASS)) {
            // 테이블 별 헤더 메타데이터 구성
            List<FileStructure.FieldInfo> fields = new ArrayList<>();
            Set<String> seen = new HashSet<>();

            // 테이블 1 메타데이터 조회
            try (
                Statement hdrStR = conn.createStatement();
                ResultSet rs0R   = hdrStR.executeQuery(
                    "SELECT * FROM " + tableR + " ORDER BY " + joinKey + " LIMIT 1")
            ) {
                ResultSetMetaData mR = rs0R.getMetaData();
                int cR = mR.getColumnCount();
                for (int i = 1; i <= cR; i++) {
                    String name = mR.getColumnName(i);
                    fields.add(new FileStructure.FieldInfo(name, 32));
                    seen.add(name);
                }
            }

            // 테이블 2 메타데이터 조회
            try (
                Statement hdrStS = conn.createStatement();
                ResultSet rs0S   = hdrStS.executeQuery(
                    "SELECT * FROM " + tableS + " ORDER BY " + joinKey + " LIMIT 1")
            ) {
                ResultSetMetaData mS = rs0S.getMetaData();
                int cS = mS.getColumnCount();
                for (int i = 1; i <= cS; i++) {
                    String name = mS.getColumnName(i);
                    if (!seen.contains(name)) {
                        fields.add(new FileStructure.FieldInfo(name, 32));
                        seen.add(name);
                    }
                }
            }

            FileStructure.Metadata fsMeta = new FileStructure.Metadata(fields);

            // 파일 헤더 기록
            try (RandomAccessFile raf = new RandomAccessFile(datFile, "rw")) {
                fileStructure.writeHeader(raf, fsMeta);
            }
            // 실제 머지 조인 결과 받아오기
            List<List<String>> rows = performMergeJoinRows(conn, tableR, tableS, joinKey);

            // 중복 없이 저장 및 출력
            for (List<String> merged : rows) {
                String keyStr = String.join("|", merged);
                if (seenMerged.add(keyStr)) {
                    System.out.println("Insert record: " + merged);
                    fileStructure.insertRecord(
                        datFile,
                        fsMeta,
                        new FileStructure.Record(merged, (byte)0)
                    );
                }
            }

            System.out.println("Merge join completed. Result saved to '" + datFile + "'.");
        }
    }

    // 내부 머지와 MySQL 머지 비교
    public void validateWithSqlJoin(String sql, String tableR, String tableS, String joinKey) throws Exception {
        List<List<String>> sqlRows = new ArrayList<>();
        try (
            Connection conn = DriverManager.getConnection(URL, USER, PASS);
            Statement st    = conn.createStatement();
            ResultSet rs    = st.executeQuery(sql)
        ) {
            ResultSetMetaData md = rs.getMetaData();
            int sqlCols = md.getColumnCount();
            
            List<Integer> useIdx = new ArrayList<>();
            for (int i = 1; i <= sqlCols; i++) {
            	useIdx.add(i);
            }
            
            while (rs.next()) {
                List<String> row = new ArrayList<>(useIdx.size());
                for (int idx : useIdx) {
                    String v = rs.getString(idx);
                    row.add(v == null ? "" : v);
                }
                sqlRows.add(row);
            }

            // 내부 머지 결과 획득
            List<List<String>> mergeRows = performMergeJoinRows(conn, tableR, tableS, joinKey);
            
            // 결과 비교
            Set<String> sqlSet   = sqlRows.stream()
                                          .map(r -> String.join("|", r))
                                          .collect(Collectors.toSet());
            Set<String> mergeSet = mergeRows.stream()
                                            .map(r -> String.join("|", r))
                                            .collect(Collectors.toSet());

            Set<String> onlyInSql   = new HashSet<>(sqlSet);
            onlyInSql.removeAll(mergeSet);
            Set<String> onlyInMerge = new HashSet<>(mergeSet);
            onlyInMerge.removeAll(sqlSet);

            // 결과 출력
            System.out.println("SQL JOIN row count:   " + sqlSet.size());
            System.out.println("Merge-Join row count: " + mergeSet.size());
            System.out.println("Only in SQL (" + onlyInSql.size() + "):");
            onlyInSql.stream().limit(10).forEach(r -> System.out.println("   " + r));
            System.out.println("Only in Merge (" + onlyInMerge.size() + "):");
            onlyInMerge.stream().limit(10).forEach(r -> System.out.println("   " + r));
            
System.out.println("=== SQL JOIN 결과 ===");
Set<String> seen = new HashSet<>();
for (List<String> row : sqlRows) {
    String key = String.join("|", row);
    if (seen.add(key)) {
        System.out.println(String.join(" | ", row));
    }
}
System.out.println("=== 내부 머지-조인 결과 ===");
seen.clear();
for (List<String> row : mergeRows) {
    String key = String.join("|", row);
    if (seen.add(key)) {
        System.out.println(String.join(" | ", row));
    }
}
        }
    }

    // ResultSet 현재 행에서 모든 컬럼 값을 읽어 List<String> 반환
    private List<String> readRow(ResultSet rs, int cols) throws SQLException {
        List<String> row = new ArrayList<>(cols);
        for (int i = 1; i <= cols; i++) {
            String v = rs.getString(i);
            row.add(v == null ? "" : v);
        }
        return row;
    }
}
