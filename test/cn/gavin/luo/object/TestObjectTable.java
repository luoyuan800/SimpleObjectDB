package cn.gavin.luo.object;

import cn.gavin.luo.object.db.IDObject;
import cn.gavin.luo.object.db.ObjectTable;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;

/**
 * Created by gluo on 6/2/2017.
 */
public class TestObjectTable {
    @Test
    public void testSaveAndClear() throws IOException {
        ObjectTable<TestClass> table = new ObjectTable<>(TestClass.class, new File("db"));
        TestClass testClass = new TestClass();
        testClass.index = 1;
        testClass.name = "test";
        table.save(testClass);
        assertEquals(new File("db", TestClass.class.getName()).list().length, 1);
        table.close();
        table = new ObjectTable<>(TestClass.class, new File("db"));
        assertEquals(new File("test").list().length, 1);
        TestClass load = table.loadObject(testClass.getId());
        assertEquals(load.id, testClass.getId());
        assertEquals(load.index, testClass.index);
        assertEquals(load.name, testClass.name);
        assertNotEquals(load, testClass);
        table.clear();
        table.close();
        assertNull(new File("db", TestClass.class.getName()).list());
    }

    @Test
    public void testUpdate() throws IOException {
        ObjectTable<TestClass> table = new ObjectTable<>(TestClass.class, new File("db"));
        TestClass testClass = new TestClass();
        testClass.index = 1;
        testClass.name = "test";
        table.save(testClass);
        testClass.name = "test1";
        table.update(testClass, testClass.getId());
        table.close();
        table = new ObjectTable<>(TestClass.class, new File("db"));
        TestClass test = table.loadObject(testClass.getId());
        assertEquals(test.name, testClass.name);
        table.clear();
    }
    @Test
    public void testSize() throws IOException {
        ObjectTable<TestClass> table = new ObjectTable<>(TestClass.class, new File("db"));
        TestClass testClass = new TestClass();
        testClass.index = 1;
        testClass.name = "test";
        table.save(testClass);
        table.close();
        table = new ObjectTable<>(TestClass.class, new File("db"));
        assertEquals(table.size(),1);
        assertEquals(table.loadAll().size(), table.size());
        table.clear();
    }

    @Test
    public void testDelete() throws IOException {
        ObjectTable<TestClass> table = new ObjectTable<>(TestClass.class, new File("db"));
        TestClass testClass = new TestClass();
        testClass.index = 1;
        testClass.name = "test";
        table.save(testClass);
        table.close();
        table = new ObjectTable<>(TestClass.class, new File("db"));
        assertEquals(table.size(),1);
        table.delete(testClass.getId());
        assertEquals(new File("db", TestClass.class.getName()).list().length, 0);
        table.clear();
    }

    @Test
    public void testFuse() throws IOException {
        ObjectTable<TestClass> table = new ObjectTable<>(TestClass.class, new File("db"));
        TestClass testClass = new TestClass();
        testClass.index = 1;
        testClass.name = "test";
        table.save(testClass);
        testClass.name = "test2";
        table.fuse();
        table.close();
        table = new ObjectTable<>(TestClass.class, new File("db"));
        assertEquals(table.loadObject(testClass.getId()).name,"test2");
        table.clear();
    }


    private static class TestClass implements Serializable, IDObject {
        int index;
        String name;
        String id;

        @Override
        public String getId() {
            return id;
        }

        @Override
        public void setId(String id) {
            this.id = id;
        }
    }
}
