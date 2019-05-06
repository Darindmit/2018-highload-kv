package ru.mail.polis.dmit;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.mapdb.serializer.SerializerArrayTuple;
import ru.mail.polis.KVDao;

import java.io.File;
import java.util.NoSuchElementException;


public class KVDaoImpl implements KVDao {

    private final DB db;
    private final HTreeMap<byte[], Value> storage;


    public KVDaoImpl(File data){
        File dataBase = new File(data, "dataBase");
        Serializer<Value> serializer = new CustomSerializer();
        this.db = DBMaker
                .fileDB(dataBase)
                .fileMmapEnableIfSupported()
                .fileMmapPreclearDisable()
                .fileChannelEnable()
                .closeOnJvmShutdown()
                .make();
        this.storage = db.hashMap(data.getName())
                .keySerializer(Serializer.BYTE_ARRAY)
                .valueSerializer(serializer)
                .createOrOpen();
    }


    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IllegalStateException {
        Value value = internalGet(key);

        if(value.getState() == Value.State.ABSENT || value.getState() == Value.State.REMOVED){
            throw new NoSuchElementException();
        }
        return value.getData();
    }

    @NotNull
    public Value internalGet(@NotNull byte[] key) {
        Value value = storage.get(key);
        if (value == null){
            return new Value(new byte[]{}, 0, Value.State.ABSENT);
        }
        return value;
    }


    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) {
        Value upsertValue = new Value(value, System.nanoTime(), Value.State.PRESENT);
        storage.put(key, upsertValue);
    }


    @Override
    public void remove(@NotNull byte[] key) {
        //storage.remove(key);
        storage.put(key, new Value(new byte[]{}, System.nanoTime(), Value.State.REMOVED));
    }


    @Override
    public void close() {
        db.close();
    }
}