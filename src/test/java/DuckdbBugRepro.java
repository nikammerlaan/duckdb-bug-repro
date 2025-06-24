import io.minio.*;
import io.minio.http.Method;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class DuckdbBugRepro {

    @Container
    private final MinIOContainer minioContainer = new MinIOContainer("minio/minio:RELEASE.2024-04-18T19-09-19Z");

    @Test
    public void test() throws Exception {
        var dbPath = createAndPopulateDatabase();
        var signedUrl = uploadAndGetSignedUrl(dbPath);
        
        attachDatabaseFromHttpUrl(signedUrl);
    }
    
    private Path createAndPopulateDatabase() throws Exception {
        var dbPath = Files.createTempFile("test_db", ".duckdb");
        Files.delete(dbPath);

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:" + dbPath)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("""
                    CREATE TABLE users AS
                    SELECT
                        row_number() OVER () as id,
                        md5(random()::VARCHAR) as name
                    FROM range(10000)
                """);
            }
        }
        
        return dbPath;
    }
    
    private String uploadAndGetSignedUrl(Path dbPath) throws Exception {
        var minioClient = MinioClient.builder()
            .endpoint(minioContainer.getS3URL())
            .credentials(minioContainer.getUserName(), minioContainer.getPassword())
            .build();
        
        var bucketName = "test-bucket";
        var makeBucketArgs = MakeBucketArgs.builder()
            .bucket(bucketName)
            .build();
        minioClient.makeBucket(makeBucketArgs);

        var objectName = "database.duckdb";
        var uploadObjectArgs = UploadObjectArgs.builder()
            .bucket(bucketName)
            .object(objectName)
            .filename(dbPath.toString())
            .contentType("application/octet-stream")
            .build();
        minioClient.uploadObject(uploadObjectArgs);

        var getPresignedObjectUrlArgs = GetPresignedObjectUrlArgs.builder()
            .method(Method.GET)
            .bucket(bucketName)
            .object(objectName)
            .expiry(3600)
            .build();
        return minioClient.getPresignedObjectUrl(getPresignedObjectUrlArgs);
    }
    
    private void attachDatabaseFromHttpUrl(String httpUrl) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("ATTACH '" + httpUrl + "' AS remote");
                
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM remote.users");
                rs.next();
                int userCount = rs.getInt(1);

                assertEquals(10_000, userCount);
            }
        }
    }
}
