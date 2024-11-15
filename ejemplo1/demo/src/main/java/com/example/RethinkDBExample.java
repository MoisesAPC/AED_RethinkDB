package com.example;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.exc.ReqlError;
import com.rethinkdb.gen.exc.ReqlQueryLogicError;
import com.rethinkdb.model.MapObject;
import com.rethinkdb.net.Connection;

import java.util.HashMap;
import java.util.Map;

/**
 * https://rethinkdb.com/docs/install-drivers/java/
 * https://rethinkdb.com/docs/quickstart/
 */

public class RethinkDBExample {
    public static final RethinkDB r = RethinkDB.r;

    public static void main(String[] args) {
        Connection conn = r.connection().hostname("localhost").port(28015).connect();

        // Creamos una base de datos llamada "test", y dentro de la misma,
        // creamos una tabla llamada "tv_shows"
        r.db("test").tableCreate("tv_shows").run(conn);
        // Insertamos una entrada a la tabla "tv_shows" cuyo campo "name" es "Star Trek TNG"
        r.table("tv_shows").insert(r.hashMap("name", "Star Trek TNG")).run(conn);
    }
}
