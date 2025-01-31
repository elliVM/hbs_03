/*
 * This file is generated by jOOQ.
 */
package com.teragrep.hbs_03.jooq.generated.journaldb.tables.records;


import com.teragrep.hbs_03.jooq.generated.journaldb.tables.Bucket;

import javax.annotation.Generated;

import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.Row2;
import org.jooq.impl.UpdatableRecordImpl;
import org.jooq.types.UShort;


/**
 * This class is generated by jOOQ.
 */
@Generated(
    value = {
        "http://www.jooq.org",
        "jOOQ version:3.12.4"
    },
    comments = "This class is generated by jOOQ"
)
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class BucketRecord extends UpdatableRecordImpl<BucketRecord> implements Record2<UShort, String> {

    private static final long serialVersionUID = 1637934011;

    /**
     * Setter for <code>journaldb.bucket.id</code>.
     */
    public void setId(UShort value) {
        set(0, value);
    }

    /**
     * Getter for <code>journaldb.bucket.id</code>.
     */
    public UShort getId() {
        return (UShort) get(0);
    }

    /**
     * Setter for <code>journaldb.bucket.name</code>. Name of the bucket
     */
    public void setName(String value) {
        set(1, value);
    }

    /**
     * Getter for <code>journaldb.bucket.name</code>. Name of the bucket
     */
    public String getName() {
        return (String) get(1);
    }

    // -------------------------------------------------------------------------
    // Primary key information
    // -------------------------------------------------------------------------

    @Override
    public Record1<UShort> key() {
        return (Record1) super.key();
    }

    // -------------------------------------------------------------------------
    // Record2 type implementation
    // -------------------------------------------------------------------------

    @Override
    public Row2<UShort, String> fieldsRow() {
        return (Row2) super.fieldsRow();
    }

    @Override
    public Row2<UShort, String> valuesRow() {
        return (Row2) super.valuesRow();
    }

    @Override
    public Field<UShort> field1() {
        return Bucket.BUCKET.ID;
    }

    @Override
    public Field<String> field2() {
        return Bucket.BUCKET.NAME;
    }

    @Override
    public UShort component1() {
        return getId();
    }

    @Override
    public String component2() {
        return getName();
    }

    @Override
    public UShort value1() {
        return getId();
    }

    @Override
    public String value2() {
        return getName();
    }

    @Override
    public BucketRecord value1(UShort value) {
        setId(value);
        return this;
    }

    @Override
    public BucketRecord value2(String value) {
        setName(value);
        return this;
    }

    @Override
    public BucketRecord values(UShort value1, String value2) {
        value1(value1);
        value2(value2);
        return this;
    }

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /**
     * Create a detached BucketRecord
     */
    public BucketRecord() {
        super(Bucket.BUCKET);
    }

    /**
     * Create a detached, initialised BucketRecord
     */
    public BucketRecord(UShort id, String name) {
        super(Bucket.BUCKET);

        set(0, id);
        set(1, name);
    }
}
