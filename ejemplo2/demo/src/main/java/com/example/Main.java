package com.example;

import java.util.List;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;

/*
 * https://rethinkdb.com/docs/install-drivers/java/
 * https://rethinkdb.com/docs/guide/java
 */

public class Main {
    // Importamos el driver
    public static final RethinkDB r = RethinkDB.r;

    public static void main(String[] args) {
        /**
         * Abrimos la conexión
         * - Hostname por defecto: "localhost"
         * - Puerto por defecto: 28015
         */
        final String hostname = "localhost";
        final int puerto = 28015;

        final String nombreBaseDeDatos = "test";
        final String nombreTabla = "authors";

        Connection conn = r.connection().hostname(hostname).port(puerto).connect();
        
        // Creamos una nueva base de datos llamada "test", y dentro de la misma,
        // creamos una nueva tabla llamada "authors"

        // Primero que nada comprobamos si la tabla ya existe. Si no es así, la creamos.
        // Primero, retornamos una lista con todas las tablas de la base de datos, y luego comprobamos
        // si ya existe una llamada "authors"
        List<String> tables = r.db(nombreBaseDeDatos).tableList().run(conn);
        Boolean tablaAuthorsNoExiste = !tables.contains(nombreTabla);

        if (tablaAuthorsNoExiste) {
            System.out.println("- Creamos una nueva base de datos llamada test");
            r.db(nombreBaseDeDatos).tableCreate(nombreTabla).run(conn);

            // Insertamos tres nuevos documentos en la tabla "authors"
            System.out.println("- Creamos una nueva tabla llamada authors, y añadimos tres entradas");
            r.table(nombreTabla).insert(r.array(
                r.hashMap("name", "William Adama")
                .with("tv_show", "Battlestar Galactica")
                .with("posts", r.array(
                    r.hashMap("title", "Decommissioning speech")
                    .with("content", "The Cylon War is long over..."),
                    r.hashMap("title", "We are at war")
                    .with("content", "Moments ago, this ship received..."),
                    r.hashMap("title", "The new Earth")
                    .with("content", "The discoveries of the past few days...")
                    )
                ),

                r.hashMap("name", "Laura Roslin")
                .with("tv_show", "Battlestar Galactica")
                .with("posts", r.array(
                    r.hashMap("title", "The oath of office")
                    .with("content", "I, Laura Roslin, ..."),
                    r.hashMap("title", "They look like us")
                    .with("content", "The Cylons have the ability...")
                    )
                ),

                r.hashMap("name", "Jean-Luc Picard")
                .with("tv_show", "Star Trek TNG")
                .with("posts", r.array(
                    r.hashMap("title", "Civil rights")
                    .with("content", "There are some words I've known since...")
                    )
                )
            )).run(conn);
        }

        // Obtenemos el número de entradas que hay de la tabla "authors"
        long numAuthors = r.table(nombreTabla).count().run(conn);
        System.out.println("- Número de entradas en la tabla authors: " + numAuthors);

        // Mostramos todas las entradas de la tabla authors
        System.out.println("- Contenido de la tabla authors:");
        Cursor cursor = r.table(nombreTabla).run(conn);
        for (Object entrada : cursor) {
            System.out.println();
            System.out.println(entrada);
            System.out.println();
        }

        // Mostramos aquella entrada cuyo campo "name" sea igual (eq) a "Laura Roslin"
        System.out.println("- Contenido de la entrada cuyo name es Laura Roslin:");
        cursor = r.table("authors")
            .filter(row -> row.g("name").eq("Laura Roslin"))
            .run(conn);
        for (Object doc : cursor) {
            System.out.println();
            System.out.println(doc);
            System.out.println();
        }

        // Actualizamos la entrada Laura Roslin para cambiarle el nombre a Pepe Martín
        r.table(nombreTabla)
            .filter(author -> author.g("name").eq("Laura Roslin"))
            .update(author -> r.hashMap("name", "Pepe Martín"))
            .run(conn);

        // Mostramos dicha entrada otra vez
        System.out.println("- Contenido de la entrada cuyo name es Pepe Martín:");
        cursor = r.table("authors")
            .filter(row -> row.g("name").eq("Pepe Martín"))
            .run(conn);
        for (Object doc : cursor) {
            System.out.println();
            System.out.println(doc);
            System.out.println();
        }

        // Ahora, borraremos la entrada cuyo nombre es Pepe Martín
        System.out.println("- Borramos la entrada Pepe Martín:");
        r.table(nombreTabla)
            .filter(author -> author.g("name").eq("Pepe Martín"))
            .delete() 
            .run(conn);

        // Mostramos todas las entradas de nuevo para ver que se ha borrado correctamente a Pepe Martín
        System.out.println("- Contenido de la tabla authors:");
        cursor = r.table(nombreTabla).run(conn);
        for (Object entrada : cursor) {
            System.out.println();
            System.out.println(entrada);
            System.out.println();
        }

        // Cerramos la conexión
        conn.close();
    }
}
