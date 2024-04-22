package cat.iesesteveterradas;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import org.bson.Document;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipFile;
import java.util.zip.ZipEntry;

public class InsertarDatosMongoDB {

    public static void main(String[] args) {
        final String zipFilePath = "./data/DISC2-LP.zip";
        final String dbName = "dam2-pj03";
        final String collectionName = "Diccionario";

        // Establece la conexión con la base de datos MongoDB
        try (MongoClient mongoClient = MongoClients.create("mongodb://elTeuUsuari:laTeuaContrasenya@192.168.151.186:27017")) {
            MongoDatabase database = mongoClient.getDatabase(dbName);
            // Abre el archivo ZIP
            try (ZipFile zipFile = new ZipFile(zipFilePath)) {
                ZipEntry entry = zipFile.getEntry("DISC2/DISC2-LP.txt"); // Asegúrate de que el nombre del archivo sea exacto

                // Lee el archivo dentro del ZIP y lo inserta en MongoDB
                if (entry != null) {
                    // Usar InputStream para leer el contenido de la entrada ZIP
                    BufferedReader reader = new BufferedReader(new InputStreamReader(zipFile.getInputStream(entry), StandardCharsets.UTF_8));
                    List<Document> documents = new ArrayList<>();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        // Crea un documento para cada línea del archivo TXT
                        Document document = new Document("idioma", "catalan")
                                            .append("palabra", line.trim())
                                            .append("veces_utilizadas", 0);
                        documents.add(document);
                    }
                    // Inserta los documentos en la colección de MongoDB
                    database.getCollection(collectionName).insertMany(documents, new InsertManyOptions().ordered(false)); // Utiliza InsertManyOptions para gestionar grandes inserciones
                    System.out.println("Datos insertados correctamente en MongoDB.");
                } else {
                    System.out.println("No se encontró el archivo dentro del ZIP.");
                }
            } catch (Exception e) {
                System.err.println("Error al leer el archivo ZIP: " + e.getMessage());
            }
        } catch (Exception e) {
            System.err.println("Error al conectar con MongoDB: " + e.getMessage());
        }
    }
}
