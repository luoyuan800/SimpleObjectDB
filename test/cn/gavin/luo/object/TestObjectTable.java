package cn.gavin.luo.object;

import cn.gavin.luo.object.db.IDModel;
import cn.gavin.luo.object.db.Index;
import cn.gavin.luo.object.db.ObjectTable;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;

/**
 * Created by gluo on 6/2/2017.
 */
public class TestObjectTable {
    @Test
    public void testSaveAndClear() throws IOException, ClassNotFoundException {
        ObjectTable<TestClass> table = null;
        try {
            table = new ObjectTable<>(TestClass.class, new File("db"));
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
        }finally {
            if(table!=null){
                table.clear();
            }
        }
    }

    @Test
    public void testUpdate() throws IOException, ClassNotFoundException {
        ObjectTable<TestClass> table = null;
        try {
            table = new ObjectTable<>(TestClass.class, new File("db"));
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
        }finally {
            if(table!=null){
                table.clear();
            }
        }
    }

    @Test
    public void testSize() throws IOException, ClassNotFoundException {
        ObjectTable<TestClass> table = null;
        try {
            table = new ObjectTable<>(TestClass.class, new File("db"));
            TestClass testClass = new TestClass();
            testClass.index = 1;
            testClass.name = "test";
            table.save(testClass);
            table.close();
            table = new ObjectTable<>(TestClass.class, new File("db"));
            assertEquals(table.size(), 1);
            assertEquals(table.loadAll().size(), table.size());
        }finally {
            if(table!=null){
                table.clear();
            }
        }
    }

    @Test
    public void testDelete() throws IOException {
        ObjectTable<TestClass> table = null;
        try {
            table = new ObjectTable<>(TestClass.class, new File("db"));
            TestClass testClass = new TestClass();
            testClass.index = 1;
            testClass.name = "test";
            table.save(testClass);
            table.close();
            table = new ObjectTable<>(TestClass.class, new File("db"));
            assertEquals(table.size(), 1);
            table.delete(testClass.getId());
            assertEquals(new File("db", TestClass.class.getName()).list().length, 0);
        }finally {
            if(table!=null){
                table.clear();
            }
        }
    }

    @Test
    public void testFuse() throws IOException, ClassNotFoundException {
        ObjectTable<TestClass> table = null;
        try {
            table = new ObjectTable<>(TestClass.class, new File("db"));
            TestClass testClass = new TestClass();
            testClass.index = 1;
            testClass.name = "test";
            table.save(testClass);
            testClass.name = "test2";
            table.fuse();
            table.close();
            table = new ObjectTable<>(TestClass.class, new File("db"));
            assertEquals(table.loadObject(testClass.getId()).name, "test2");
        }finally {
            if(table!=null){
                table.clear();
            }
        }
    }

    @Test
    public void testLoadLimit() throws IOException, ClassNotFoundException {
        ObjectTable<TestClass> table = null;
        try {
            table = new ObjectTable<>(TestClass.class, new File("db"));
            TestClass testClass = new TestClass();
            testClass.index = 1;
            testClass.name = "test";
            table.save(testClass);
            testClass = new TestClass();
            testClass.index = 1;
            testClass.name = "test1";
            table.save(testClass);
            testClass = new TestClass();
            testClass.index = 1;
            testClass.name = "test1";
            table.save(testClass);
            testClass = new TestClass();
            testClass.index = 1;
            testClass.name = "test1";
            table.save(testClass);
            assertEquals(table.loadLimit(0, 3, new Index<TestClass>() {
                @Override
                public boolean match(TestClass testClass) {
                    return testClass.name.equals("test1");
                }
            }, null).size(), 3);
            assertEquals(table.loadLimit(0, 4, new Index<TestClass>() {
                @Override
                public boolean match(TestClass testClass) {
                    return testClass.name.equals("test1");
                }
            }, null).size(), 3);
            assertEquals(table.loadLimit(0, 2, new Index<TestClass>() {
                @Override
                public boolean match(TestClass testClass) {
                    return testClass.name.equals("test1");
                }
            }, null).size(), 2);
            assertEquals(table.loadLimit(1, 2, new Index<TestClass>() {
                @Override
                public boolean match(TestClass testClass) {
                    return testClass.name.equals("test1");
                }
            }, null).size(), 2);
            assertEquals(table.loadLimit(2, 2, new Index<TestClass>() {
                @Override
                public boolean match(TestClass testClass) {
                    return testClass.name.equals("test1");
                }
            }, null).size(), 1);
            assertEquals(table.loadLimit(3, 2, new Index<TestClass>() {
                @Override
                public boolean match(TestClass testClass) {
                    return testClass.name.equals("test1");
                }
            }, null).size(), 0);
            assertEquals(table.loadLimit(3, 3, new Index<TestClass>() {
                @Override
                public boolean match(TestClass testClass) {
                    return testClass.name.equals("test1");
                }
            }, null).size(), 0);
        }finally {
            if(table!=null){
                table.clear();
            }
        }
    }


    @Test
    public void testLoadLimitAndOrder() throws IOException, ClassNotFoundException {
        ObjectTable<TestClass> table = null;
        try {
            table = new ObjectTable<>(TestClass.class, new File("db"));
            TestClass testClass = new TestClass();
            testClass.index = 1;
            testClass.name = "test";
            table.save(testClass);
            testClass = new TestClass();
            testClass.index = 2;
            testClass.name = "test1";
            table.save(testClass);
            testClass = new TestClass();
            testClass.index = 4;
            testClass.name = "test1";
            table.save(testClass);
            testClass = new TestClass();
            testClass.index = 5;
            testClass.name = "test1";
            table.save(testClass);
            testClass = new TestClass();
            testClass.index = 3;
            testClass.name = "test1";
            table.save(testClass);
            testClass = new TestClass();
            testClass.index = 7;
            testClass.name = "test1";
            table.save(testClass);
            testClass = new TestClass();
            testClass.index = 6;
            testClass.name = "test1";
            table.save(testClass);
            List<TestClass> result = table.loadLimit(1, 3, new Index<TestClass>() {
                @Override
                public boolean match(TestClass testClass) {
                    return testClass.name.equals("test1");
                }
            }, new Comparator<TestClass>() {
                @Override
                public int compare(TestClass o1, TestClass o2) {
                    return Integer.valueOf(o1.index).compareTo(o2.index);
                }
            });
            assertEquals(result.get(0).index, 3);
            assertEquals(result.get(1).index, 4);
            assertEquals(result.get(2).index, 5);
        }finally {
            if(table!=null){
                table.clear();
            }
        }
    }

    private static class TestClass implements Serializable, IDModel {
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
