package cn.gavin.luo.object.db;

/**
 * Created by gluo on 6/5/2017.
 */
public interface Index<T> {
    boolean match(T t);
}
