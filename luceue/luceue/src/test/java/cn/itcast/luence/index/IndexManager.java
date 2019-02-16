package cn.itcast.luence.index;

import cn.itcast.lucene.dao.BookDao;
import cn.itcast.lucene.dao.Impl.BookDaoImpl;
import cn.itcast.lucene.pojo.Book;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.junit.Test;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class IndexManager {
    /**
     * 创建索引
     * @throws Exception
     */
    @Test
    public void  createIndex() throws Exception {
    //采集数据
        BookDao bookDao = new BookDaoImpl();
        List<Book> bookList = bookDao.findAll();
        System.out.println(bookList);
    //创建文档对象（Document）,并且封装文档对象
        List<Document> list = new ArrayList<>();
        for (Book book : bookList) {
            Document document = new Document();
            /**
             * 图书id
             * 是否分词：不需要分词
             是否索引：需要索引
             是否存储：需要存储
             -- StringField
             */
            document.add(new StringField("id", book.getId() + "", Field.Store.YES));
            /**
             * 图书名称
             是否分词：需要分词
             是否索引：需要索引
             是否存储：需要存储
             -- TextField
             */
            document.add(new TextField("bookName",book.getBookName(), Field.Store.YES));
            /**
             * 图书价格
             是否分词：（数值型的Field lucene使用内部的分词）
             是否索引：需要索引
             是否存储：需要存储
             -- DoubleField
             */
            document.add(new DoubleField("bookPrice",book.getPrice(), Field.Store.YES));
            /**
             * 图书图片
             是否分词：不需要分词
             是否索引：不需要索引
             是否存储：需要存储
             -- StoredField
             */
            document.add(new StoredField("bookPic",book.getPic()));
            /**
             * 图书描述
             是否分词：需要分词
             是否索引：需要索引
             是否存储：不需要存储
             -- TextField
             */
            document.add(new TextField("bookDesc",book.getBookDesc(), Field.Store.NO));
            list.add(document);
        }
    //创建分析器（Analyzer），用于分词
        Analyzer analyzer = new IKAnalyzer();
    //创建索引库配置对象（IndexWriterConfig），配置索引库
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_10_3,analyzer);
        //设置索引库打开的方式
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    //创建索引库目录对象（Directory），指定索引库的位置
        Directory directory = FSDirectory.open(new File("D:/lucene_Test"));
    //创建索引库操作对象（IndexWriter），把文档对象写入索引库
        IndexWriter indexWriter = new IndexWriter(directory,config);
    //提交事务
        //循环文档列表,写入索引库
        for (Document document : list) {
            //添加文档到库中
            indexWriter.addDocument(document);
            indexWriter.commit();
        }
    //释放资源
        indexWriter.close();

    }

    /**
     * 查询索引结果
     * @throws Exception
     */
    @Test
    public void searchIndex() throws Exception {
    //创建分析器对象（Analyzer），用于分词
        Analyzer analyzer = new IKAnalyzer();
    //创建查询对象（Query）
        //创建解析器对象:参数为搜索类型和分词器
        QueryParser queryParser = new QueryParser("bookName",analyzer);
        Query query = queryParser.parse("bookName:java");
    //创建索引库的目录（Directory），指定索引库的位置
        Directory directory = FSDirectory.open(new File("D:/lucene_Test"));
    //创建索引读取对象（IndexReader），把索引数据读取到内存中
        IndexReader indexReader = DirectoryReader.open(directory);
    //创建索引搜索对象（IndexSearcher），执行搜索
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);
    //使用IndexSearcher执行搜索，返回搜索结果集（TopDocs）
        TopDocs topDocs = indexSearcher.search(query,10);
        System.out.println("总命中数:"+topDocs.totalHits);
    //处理结果集
        //获取文档的搜索数组
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        for (ScoreDoc scoreDoc : scoreDocs) {
            System.out.println("----------华丽的分割线-----------");
            System.out.println("文档id:"+scoreDoc.doc+","+"文档分值:"+scoreDoc.score);
            //根据doC.ID选择指定的存储文档
            Document document =indexSearcher.doc(scoreDoc.doc);
            System.out.println("图书Id：" + document.get("id"));
            System.out.println("图书名称：" + document.get("bookName"));
            System.out.println("图书价格：" + document.get("bookPrice"));
            System.out.println("图书图片：" + document.get("bookPic"));
            System.out.println("图书描述：" + document.get("bookDesc"));
        }
        //释放资源
        indexReader.close();
    }

    /**
     * 删除文档
     * @throws Exception
     */
    @Test
    public void deleteIndex() throws Exception{
        Analyzer analyzer = new IKAnalyzer();
        IndexWriterConfig config =new IndexWriterConfig(Version.LUCENE_4_10_3,analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        Directory directory = FSDirectory.open(new File("D:/lucene_Test"));
        //根据IndexWriter反推前面的
        IndexWriter indexWriter = new IndexWriter(directory,config);
        Term term =new Term("bookName","java");
        indexWriter.deleteDocuments(term);
        indexWriter.close();
    }

    /**
     * 删除全部文档和索引
     * @throws Exception
     */
    @Test
    public void deleteAll() throws Exception {
        Analyzer analyzer = new IKAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_10_3,analyzer);
        Directory directory =FSDirectory.open(new File("D:/lucene_Test"));
        IndexWriter indexWriter = new IndexWriter(directory,config);
        indexWriter.deleteAll();
        indexWriter.close();
    }

    /**
     * 更新文档
     */
    @Test
    public void updateDocument() throws  Exception{
        Analyzer analyzer = new IKAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_4_10_3,analyzer);
        Directory directory = FSDirectory.open(new File("D:/lucene_Test"));
        IndexWriter indexWriter = new IndexWriter(directory,config);

        Document doc = new Document();
        doc.add(new StringField("id","20",Field.Store.YES));
        //doc.add(new TextField("name","lucene solr zookeeper",Field.Store.YES ));

        Term term = new Term("bookName","java");

        indexWriter.updateDocument(term,doc);
        indexWriter.commit();
        indexWriter.close();
    }

    /**
     * 查询的方法
     */
    private void search(Query query) throws Exception{
        System.out.println("查询的语法:"+query);
        Directory directory = FSDirectory.open(new File("D:/lucene_Test"));
        //读取索引库对象
        IndexReader indexReader =DirectoryReader.open(directory);
        //获得执行索引库的indexSearch
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);
        /**
         * search:返回结果集
         * 第一个参数:Query查询对象
         * 第二个参数:查询搜索结果的前n个
         */
        TopDocs topDocs = indexSearcher.search(query, 10);
        //处理结果集
        System.out.println("总命中的记录数:"+topDocs.totalHits);
        //获得搜索到的文档数组
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;
        for (ScoreDoc scoreDoc : scoreDocs) {
            System.out.println("============华丽的分割线=============");
            System.out.println("文档id:"+scoreDoc.doc+"\t文档分值:"+scoreDoc.score);
            Document document = indexSearcher.doc(scoreDoc.doc);
            System.out.println("图书id:"+document.get("id"));
            System.out.println("图书名称:"+document.get("bookName"));
            System.out.println("图书价格:"+document.get("bookPrice"));
            System.out.println("图书图片:"+document.get("bookPic"));
            System.out.println("图书描述:"+document.get("bookDesc"));
        }
        indexReader.close();
    }

    /**
     * 传入查询的条件测试1
     */
    @Test
    public void testTermQuery() throws Exception{
        //指定名字查询
        TermQuery q1= new TermQuery(new Term("bookName", "java"));
        //价格范围查询
        Query q2 = NumericRangeQuery.newDoubleRange("bookPrice", 80d,100d,true,true);

        //联合查询
        BooleanQuery query =new BooleanQuery();
        query.add(q1,BooleanClause.Occur.MUST);
        query.add(q2,BooleanClause.Occur.MUST);

        search(query);
    }

    /**
     * 传入查询条件测试2
     */
    @Test
    public void  testTermQuery2() throws Exception{
        Analyzer analyzer = new IKAnalyzer();
        QueryParser queryParser = new QueryParser("bookName",analyzer);
        /**
         * 如果用单词必须要大写,用符号的话在前面加符号
         * AND :+
         * OR:空格
         * NOT:-
         */
        Query q = queryParser.parse("+bookName:java +bookName:lucene");
        search(q);
    }
}
