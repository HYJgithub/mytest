package cn.itcast.lucene.dao;

import cn.itcast.lucene.pojo.Book;

import java.util.List;

public interface BookDao {
    List<Book> findAll();
}
