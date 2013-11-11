package org.elasticsearch.index.mapper.xcontent;

import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.attachment.AttachmentMapper;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;

public class ContentRefTest {

	private DocumentMapperParser mapperParser;

	@BeforeClass
	public void beforeClass() {
        mapperParser = new DocumentMapperParser(new Index("test"), new AnalysisService(new Index("test")), null, null);
        mapperParser.putTypeParser(AttachmentMapper.CONTENT_TYPE, new AttachmentMapper.TypeParser());
	}

	@Test(enabled=true)
	public void testContentRef() throws IOException {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/contentRef/test-mapping.json");
        DocumentMapper docMapper = mapperParser.parse(mapping);

        BytesReference json = jsonBuilder().startObject().field("_id", 1).field("our_url", "7c/7c8b8d2aa874aa247210ce1821495e3e").endObject().bytes();

        Document doc = docMapper.parse(json).rootDoc();
        assertThat(doc.get(docMapper.mappers().smartName("our_url").mapper().names().indexName()), containsString("Выбор кредитной организации"));

	}

	@Test(enabled=true)
	public void testContentRef2() throws IOException {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/contentRef/test-mapping2.json");
        byte[] docData = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/contentRef/test-doc.json");
        DocumentMapper docMapper = mapperParser.parse(mapping);

        List<Document> docs = docMapper.parse(new BytesArray(docData)).docs();
        //System.out.println(docs.get(9).get(docMapper.mappers().smartName("our_url").mapper().names().indexName()));
        assertThat(docs.get(2).get(docMapper.mappers().smartName("our_url").mapper().names().indexName()), containsString("При подготовке проектов планировки"));

	}

	@Test(enabled=true)
	public void testContentRefArchive() throws IOException, TikaException {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/contentRef/test-mapping2.json");
        byte[] docData = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/contentRef/test-doc-arch1.json");
        DocumentMapper docMapper = mapperParser.parse(mapping);

        List<Document> docs = docMapper.parse(new BytesArray(docData)).docs();
        assertThat(docs.get(3).getValues(docMapper.mappers().smartName("our_url").mapper().names().indexName())[2], containsString("на инженерно-геологические изыскания"));
        assertThat(docs.get(5).get(docMapper.mappers().smartName("our_url").mapper().names().indexName()), containsString("Об утверждении Перечня видов работ по инженерным изысканиям"));
        //System.out.println(new Tika().parseToString(new File("/home/skingreek/tst/61/61a24aecb376a9104c6fd0769ca4da78")));
	}
}
