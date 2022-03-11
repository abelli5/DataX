package com.alibaba.datax.plugin.rdbms.writer.util;

import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.sun.tools.javac.util.List;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

public class WriterUtilTest {

    @Test
    public void onDuplicateKeyUpdateString4PostgresqlFailure() {
        try {
            WriterUtil.onDuplicateKeyUpdateString4Postgresql(List.of("user_id", "page_id"),
                    List.of("user_id", "page_id", "enabled"));
            fail("should IAE be thrown");
        } catch (IllegalArgumentException e) {
            // good
        }
    }

    @Test
    public void onDuplicateKeyUpdateString4Postgresql() {
        Assert.assertEquals(" ON CONFLICT (user_id,page_id) DO UPDATE SET enabled=EXCLUDED.enabled",
                WriterUtil.onDuplicateKeyUpdateString4Postgresql(List.of("user_id", "page_id", "enabled"),
                        List.of("user_id", "page_id")));
    }

    @Test
    public void onDuplicateKeyDoNothing4Postgresql() {
        Assert.assertEquals(" ON CONFLICT (user_id,page_id) DO NOTHING",
                WriterUtil.onDuplicateKeyDoNothing4Postgresql(List.of("user_id", "page_id")));
    }

    @Test
    public void getWriteTemplate4PostgresqlUpdate() {
        Assert.assertEquals("INSERT INTO %s (user_id,page_id,enabled) VALUES(?,?,?) ON CONFLICT (user_id,page_id) DO UPDATE SET enabled=EXCLUDED.enabled",
                WriterUtil.getWriteTemplate4Postgresql(List.of("user_id", "page_id", "enabled"),
                        List.of("?", "?", "?"),
                        List.of("user_id", "page_id"),
                        "update", DataBaseType.PostgreSQL, false));
    }

    @Test
    public void getWriteTemplate4PostgresqlReplace() {
        Assert.assertEquals("INSERT INTO %s (user_id,page_id,enabled) VALUES(?,?,?) ON CONFLICT (user_id,page_id) DO UPDATE SET enabled=EXCLUDED.enabled",
                WriterUtil.getWriteTemplate4Postgresql(List.of("user_id", "page_id", "enabled"),
                        List.of("?", "?", "?"),
                        List.of("user_id", "page_id"),
                        "replace", DataBaseType.PostgreSQL, false));
    }

    @Test
    public void getWriteTemplate4PostgresqlInsert() {
        Assert.assertEquals("INSERT INTO %s (user_id,page_id,enabled) VALUES(?,?,?) ON CONFLICT (user_id,page_id) DO NOTHING",
                WriterUtil.getWriteTemplate4Postgresql(List.of("user_id", "page_id", "enabled"),
                        List.of("?", "?", "?"),
                        List.of("user_id", "page_id"),
                        "insert", DataBaseType.PostgreSQL, true));
    }
}