package cn.gavin.luo.object.db;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

/**
 * Created by gluo on 11/28/2016.
 */
public class ObjectTable<T extends Serializable> {
    private File root;
    private Class<T> table;
    private HashMap<String, SoftReference<T>> cache;

    public ObjectTable(Class<T> table, File root) {
        this.root = new File(root,table.getSimpleName());
        this.table = table;
        cache = new HashMap<String, SoftReference<T>>();
    }

    public synchronized String save(T object, String id) throws IOException {
        File entry = buildFile(id);
        if(entry.exists()){
            throw new IOException("Object with id: " + id + " already existed!");
        }
        saveObject(object, entry);
        cache.put(id, new SoftReference<T>(object));
        return id;
    }

    public synchronized String update(T object, String id){
        File file = buildFile(id);
        if(file.exists()){
            file.delete();
        }
        saveObject(object, file);
        return id;
    }

    private void saveObject(T object, File entry) {
        try {
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(entry));
            oos.writeObject(object);
            oos.flush();
            oos.close();
        } catch (IOException e) {
            //LogHelper.logException(e,"ObjectDb->save{" + object + ", " + id + "}");
        }
    }

    public String save(T object) throws IOException {
        if(object instanceof IDObject){
            if(((IDObject) object).getId() == null){
                ((IDObject) object).setId(UUID.randomUUID().toString());
            }
            return save(object, ((IDObject) object).getId());
        }else {
            return save(object, UUID.randomUUID().toString());
        }
    }

    public synchronized T loadObject(String id) {
        T object = null;
        SoftReference<T> ref = cache.get(id);
        if(ref!=null){
            object = ref.get();
        }
        if(object == null) {
            String name = getName(id);
            object = load(name);
        }
        return object;
    }

    public List<T> loadAll(){
        List<T> list = new ArrayList<>();

        return list;
    }

    public synchronized void clear() {


    }

    public void delete(String id) {
        buildFile(id).delete();
        cache.remove(id);
    }

    public void fuse() throws IOException {
        for(SoftReference<T> ref : cache.values()){
            if(ref!=null && ref.get()!=null){
                if(ref.get() instanceof IDObject){
                    save(ref.get(),((IDObject) ref.get()).getId());
                }
            }
        }
    }

    private String getName(String id) {
        return table.getName() + "@" + id;
    }

    private T load(String id) {
        File entry = buildFile(id);
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(entry))){
            Object o = ois.readObject();
            ois.close();
            return table.cast(o);
        } catch (IOException | ClassNotFoundException e) {
            //LogHelper.logException(e,"Sqlite->load{" + name + "}");
        }
        return null;
    }

    private File buildFile(String id){
        return new File(root,id);
    }

}
