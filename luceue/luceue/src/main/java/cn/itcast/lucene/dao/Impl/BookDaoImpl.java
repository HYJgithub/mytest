package cn.itcast.lucene.dao.Impl;

import cn.itcast.lucene.dao.BookDao;
import cn.itcast.lucene.pojo.Book;


import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class BookDaoImpl  implements BookDao{
    /**
     * 接口实现从数据库获得数据
     * @return
     */
    @Override
    public List<Book> findAll() {
        List<Book> list =  new ArrayList<>();
        Connection connection=null;
        PreparedStatement psmt= null;
        ResultSet rs =null;
        try {
        //加载驱动
            Class.forName("com.mysql.jdbc.Driver");
        //创建数据库连接对象
            connection = DriverManager.
                    getConnection("jdbc:mysql://localhost:3306/lucene_db","root","root");
        //编写sql语句
            String sql="select * from book";
        //创建statement
            psmt = connection.prepareStatement(sql);
        //执行查询,获得结果集
           rs = psmt.executeQuery();
        //处理结果集(封装类)
            while(rs.next()){
                Book book = new Book();
                book.setId(rs.getInt("id"));
                book.setBookName(rs.getString("bookname"));
                book.setPrice(rs.getFloat("price"));
                book.setPic(rs.getString("pic"));
                book.setBookDesc(rs.getString("bookdesc"));
                list.add(book);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            // 释放资源
               try {
           if(rs != null) rs.close();
           if(psmt != null)psmt.close();
           if(connection != null) connection.close();
               } catch (Exception e) {
                   e.printStackTrace();
               }
        }
        return list;
    }
}
