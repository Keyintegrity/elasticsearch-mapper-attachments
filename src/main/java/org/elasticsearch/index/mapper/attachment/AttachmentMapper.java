/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper.attachment;

import static org.elasticsearch.index.mapper.MapperBuilders.dateField;
import static org.elasticsearch.index.mapper.MapperBuilders.integerField;
import static org.elasticsearch.index.mapper.MapperBuilders.stringField;
import static org.elasticsearch.index.mapper.attachment.MapperBuilders.bigStringField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parsePathType;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParserDecorator;
import org.apache.tika.sax.BodyContentHandler;
import org.apache.tika.sax.WriteOutContentHandler;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.FieldMapperListener;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeContext;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.ObjectMapperListener;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.core.IntegerFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.multifield.MultiFieldMapper;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 * <pre>
 *      field1 : "..."
 * </pre>
 * <p>Or:
 * <pre>
 * {
 *      file1 : {
 *          _content_type : "application/pdf",
 *          _content_length : "500000000",
 *          _name : "..../something.pdf",
 *          content : ""
 *      }
 * }
 * </pre>
 * <p/>
 * _content_length = Specify the maximum amount of characters to extract from the attachment. If not specified, then the default for
 * tika is 100,000 characters. Caution is required when setting large values as this can cause memory issues.
 */
public class AttachmentMapper implements Mapper {

	private static final GenericObjectPool<StringBuilder> sbPool = new GenericObjectPool<StringBuilder>(new StringBuilderFactory());

	private static class StringBuilderFactory extends BasePooledObjectFactory<StringBuilder> {

		@Override
		public StringBuilder create() throws Exception {
			return new StringBuilder();
		}

		@Override
		public PooledObject<StringBuilder> wrap(StringBuilder builder) {
			return new DefaultPooledObject<StringBuilder>(builder);
		}
		
		@Override
		public void passivateObject(PooledObject<StringBuilder> pooledObject)
				throws Exception {
			pooledObject.getObject().setLength(0);
		}

	}

	private class RecursiveMetadataParser extends ParserDecorator {
		private static final long serialVersionUID = -8317176389312877060L;
		
		private ParseContext indexContext;
		private int indexedChars;
		private String indexName;

		public RecursiveMetadataParser(Parser parser, ParseContext context, int indexedChars, String name) {
			super(parser);
			this.indexContext = context;
			this.indexedChars = indexedChars;
			indexName = name;
		}

		@Override
		public void parse(InputStream stream, ContentHandler ignored,
				Metadata metadata, org.apache.tika.parser.ParseContext context)
				throws IOException, SAXException, TikaException {
			StringBuilder contentData = null;
			boolean isPooled = true;
			try {
				contentData = sbPool.borrowObject();
			} catch (Exception e1) {
				contentData = new StringBuilder(1024);
				isPooled = false;
			}
	        WriteOutContentHandler handler = new WriteOutContentHandler(new StringBuilderWriter(contentData), indexedChars);
			try {
				super.parse(stream, new BodyContentHandler(handler), metadata, context);
			} catch (Throwable e) {
				if (!handler.isWriteLimitReached(e)) {
		            // #18: we could ignore errors when Tika does not parse data
		            if (!ignoreErrors) throw new MapperParsingException("Failed to extract text for [" + indexName + "]", e);
		            return;
				}
			}
//			System.out.println("metadata:" + metadata);
//			System.out.println("contentData:" + contentData.length());
			if (contentData.length() > 0) {
				indexContext.externalValue(contentData);
				contentMapper.parse(indexContext);
			}
			if (isPooled) {
				sbPool.returnObject(contentData);
			}
		}
	}

	private static ESLogger logger = ESLoggerFactory.getLogger(AttachmentMapper.class.getName());

    public static final String CONTENT_TYPE = "attachment";

    public static class Defaults {
        public static final ContentPath.Type PATH_TYPE = ContentPath.Type.FULL;
    }

    public static class Builder extends Mapper.Builder<Builder, AttachmentMapper> {

        private ContentPath.Type pathType = Defaults.PATH_TYPE;

        private Integer defaultIndexedChars = null;

        private Boolean ignoreErrors = null;

        private String contentRefRoot = null;

        private Mapper.Builder contentBuilder;

        private Mapper.Builder titleBuilder = stringField("title");

        private Mapper.Builder nameBuilder = stringField("name");

        private Mapper.Builder authorBuilder = stringField("author");

        private Mapper.Builder keywordsBuilder = stringField("keywords");

        private Mapper.Builder dateBuilder = dateField("date");

        private Mapper.Builder contentTypeBuilder = stringField("content_type");

        private Mapper.Builder contentLengthBuilder = integerField("content_length");

        public Builder(String name) {
            super(name);
            this.builder = this;
            this.contentBuilder = bigStringField(name);
        }

        public Builder pathType(ContentPath.Type pathType) {
            this.pathType = pathType;
            return this;
        }

        public Builder content(Mapper.Builder content) {
            this.contentBuilder = content;
            return this;
        }

        public Builder date(Mapper.Builder date) {
            this.dateBuilder = date;
            return this;
        }

        public Builder author(Mapper.Builder author) {
            this.authorBuilder = author;
            return this;
        }

        public Builder title(Mapper.Builder title) {
            this.titleBuilder = title;
            return this;
        }

        public Builder name(Mapper.Builder name) {
            this.nameBuilder = name;
            return this;
        }

        public Builder keywords(Mapper.Builder keywords) {
            this.keywordsBuilder = keywords;
            return this;
        }

        public Builder contentType(Mapper.Builder contentType) {
            this.contentTypeBuilder = contentType;
            return this;
        }

        public Builder contentLength(Mapper.Builder contentType) {
            this.contentLengthBuilder = contentType;
            return this;
        }
        
		public Builder contentRefRoot(String contentRefRoot) {
            this.contentRefRoot = contentRefRoot;
            return this;
		}


        @Override
        public AttachmentMapper build(BuilderContext context) {
            //logger.info("Builder.build " + contentRefRoot + " " + this);
            ContentPath.Type origPathType = context.path().pathType();
            context.path().pathType(pathType);

            // create the content mapper under the actual name
            Mapper contentMapper = contentBuilder.build(context);

            // create the DC one under the name
            context.path().add(name);
            Mapper dateMapper = dateBuilder.build(context);
            Mapper authorMapper = authorBuilder.build(context);
            Mapper titleMapper = titleBuilder.build(context);
            Mapper nameMapper = nameBuilder.build(context);
            Mapper keywordsMapper = keywordsBuilder.build(context);
            Mapper contentTypeMapper = contentTypeBuilder.build(context);
            Mapper contentLength = contentLengthBuilder.build(context);
            context.path().remove();

            context.path().pathType(origPathType);

            if (defaultIndexedChars == null && context.indexSettings() != null) {
                defaultIndexedChars = context.indexSettings().getAsInt("index.mapping.attachment.indexed_chars", 100000);
            }
            if (defaultIndexedChars == null) {
                defaultIndexedChars = 100000;
            }

            if (ignoreErrors == null && context.indexSettings() != null) {
                ignoreErrors = context.indexSettings().getAsBoolean("index.mapping.attachment.ignore_errors", Boolean.TRUE);
            }
            if (ignoreErrors == null) {
                ignoreErrors = Boolean.TRUE;
            }

            return new AttachmentMapper(name, pathType, defaultIndexedChars, ignoreErrors, contentRefRoot, contentMapper, dateMapper, titleMapper, nameMapper, authorMapper, keywordsMapper, contentTypeMapper, contentLength);
        }
    }

    /**
     * <pre>
     *  field1 : { type : "attachment" }
     * </pre>
     * Or:
     * <pre>
     *  field1 : {
     *      type : "attachment",
     *      fields : {
     *          field1 : {type : "binary"},
     *          title : {store : "yes"},
     *          date : {store : "yes"},
     *          name : {store : "yes"},
     *          author : {store : "yes"},
     *          keywords : {store : "yes"},
     *          content_type : {store : "yes"},
     *          content_length : {store : "yes"}
     *      }
     * }
     * </pre>
     */
    public static class TypeParser implements Mapper.TypeParser {

        @SuppressWarnings({"unchecked"})
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            AttachmentMapper.Builder builder = new AttachmentMapper.Builder(name);
            //logger.info("TypeParser.parse " + name + node);

            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (fieldName.equals("path")) {
                    builder.pathType(parsePathType(name, fieldNode.toString()));
                } else if (fieldName.equals("fields")) {
                    Map<String, Object> fieldsNode = (Map<String, Object>) fieldNode;
                    for (Map.Entry<String, Object> entry1 : fieldsNode.entrySet()) {
                        String propName = entry1.getKey();
                        Object propNode = entry1.getValue();

                        // Check if we have a multifield here
                        boolean isMultifield = false;
                        boolean isString = false;
                        if (propNode != null && propNode instanceof Map) {
                            Object oType = ((Map<String, Object>) propNode).get("type");
                            if (oType != null && oType.equals(MultiFieldMapper.CONTENT_TYPE)) {
                                isMultifield = true;
                            }
                            if (oType != null && oType.equals(StringFieldMapper.CONTENT_TYPE)) {
                                isString = true;
                            }
                        }

                        if (name.equals(propName)) {
                            // that is the content
                            builder.content(parserContext.typeParser(isMultifield? MultiFieldMapper.CONTENT_TYPE:BigStringFieldMapper.CONTENT_TYPE).parse(name, (Map<String, Object>) propNode, parserContext));
                        } else if ("date".equals(propName)) {
                            // If a specific format is already defined here, we should use it
                            builder.date(parserContext.typeParser(isMultifield ? MultiFieldMapper.CONTENT_TYPE : isString ? StringFieldMapper.CONTENT_TYPE : DateFieldMapper.CONTENT_TYPE).parse("date", (Map<String, Object>) propNode, parserContext));
                        } else if ("title".equals(propName)) {
                            builder.title(parserContext.typeParser(isMultifield? MultiFieldMapper.CONTENT_TYPE:StringFieldMapper.CONTENT_TYPE).parse("title", (Map<String, Object>) propNode, parserContext));
                        } else if ("name".equals(propName)) {
                            builder.name(parserContext.typeParser(isMultifield? MultiFieldMapper.CONTENT_TYPE:StringFieldMapper.CONTENT_TYPE).parse("name", (Map<String, Object>) propNode, parserContext));
                        } else if ("author".equals(propName)) {
                            builder.author(parserContext.typeParser(isMultifield? MultiFieldMapper.CONTENT_TYPE:StringFieldMapper.CONTENT_TYPE).parse("author", (Map<String, Object>) propNode, parserContext));
                        } else if ("keywords".equals(propName)) {
                            builder.keywords(parserContext.typeParser(isMultifield? MultiFieldMapper.CONTENT_TYPE:StringFieldMapper.CONTENT_TYPE).parse("keywords", (Map<String, Object>) propNode, parserContext));
                        } else if ("content_type".equals(propName)) {
                            builder.contentType(parserContext.typeParser(isMultifield? MultiFieldMapper.CONTENT_TYPE:StringFieldMapper.CONTENT_TYPE).parse("content_type", (Map<String, Object>) propNode, parserContext));
                        } else if ("content_length".equals(propName)) {
                            builder.contentLength(parserContext.typeParser(isMultifield? MultiFieldMapper.CONTENT_TYPE: IntegerFieldMapper.CONTENT_TYPE).parse("content_length", (Map<String, Object>) propNode, parserContext));
                        }
                    }
                } else if (fieldName.equals("contentRefRoot")) {
                	String contentRefRoot = (String)fieldNode;
                	if (contentRefRoot != null) {
	                	File contentRefRootFile = new File(contentRefRoot);
	                	if (!contentRefRootFile.isAbsolute()) {
	                		try {
	                			contentRefRootFile = new File(this.getClass().getClassLoader().getResource(contentRefRoot).toURI());
							} catch (URISyntaxException e) {
								throw new MapperParsingException("invalid contentRefRoot", e);
							}
	                	}
	                	if (!contentRefRootFile.exists()) {
							throw new MapperParsingException("invalid contentRefRoot. '" + contentRefRootFile.getAbsolutePath() + "' does not exists.");
	                	}
	                	builder.contentRefRoot(contentRefRootFile.getAbsolutePath());
                	} else {
                		builder.contentRefRoot(contentRefRoot);
                	}
                    //logger.info("contentRefRoot: {}", fieldNode);
                }
            }

            return builder;
        }
    }

    private final String name;

    private final ContentPath.Type pathType;

    private final int defaultIndexedChars;

    private final boolean ignoreErrors;
    
    private String contentRefRoot; 

    private final Mapper contentMapper;

    private final Mapper dateMapper;

    private final Mapper authorMapper;

    private final Mapper titleMapper;

    private final Mapper nameMapper;

    private final Mapper keywordsMapper;

    private final Mapper contentTypeMapper;

    private final Mapper contentLengthMapper;

    public AttachmentMapper(String name, ContentPath.Type pathType, int defaultIndexedChars, Boolean ignoreErrors, String contentRefRoot, Mapper contentMapper,
                            Mapper dateMapper, Mapper titleMapper, Mapper nameMapper, Mapper authorMapper,
                            Mapper keywordsMapper, Mapper contentTypeMapper, Mapper contentLengthMapper) {
        this.name = name;
        this.pathType = pathType;
        this.defaultIndexedChars = defaultIndexedChars;
        this.ignoreErrors = ignoreErrors;
		this.contentRefRoot = contentRefRoot;
        this.contentMapper = contentMapper;
        this.dateMapper = dateMapper;
        this.titleMapper = titleMapper;
        this.nameMapper = nameMapper;
        this.authorMapper = authorMapper;
        this.keywordsMapper = keywordsMapper;
        this.contentTypeMapper = contentTypeMapper;
        this.contentLengthMapper = contentLengthMapper;
    }

    @Override
    public String name() {
        return name;
    }
    
	private long sum_time = 0;

	private int docs_count = 0;

	private Parser rootTikaParser = new AutoDetectParser();

    @Override
    public void parse(ParseContext context) throws IOException {
    	long stat_time = System.nanoTime();
        InputStream content = null;
        long dataLength = -1;
        String contentType = null;
        int indexedChars = defaultIndexedChars;
        String name = null;

        XContentParser parser = context.parser();
        //context.
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_STRING) {
            //logger.info("contentRefRoot: {}", contentRefRoot);
        	if (contentRefRoot != null) {
        		try {
//        			System.out.println("fileName:" + parser.text());
        	        //logger.info("parse " + parser.text());
        			name = parser.text();
        			File file = new File(contentRefRoot, name);
        			content = new FileInputStream(file);
        			dataLength = file.length();
        		} catch (IOException e) {
                    if (!ignoreErrors) throw new MapperParsingException("Failed to resolve file path [" + contentRefRoot + "/" + name + "]", e);
                    if (logger.isDebugEnabled()) logger.debug("Ignoring IOException catch while parsing content: {}: {}", e.getMessage(), contentRefRoot + "/" + name);
                    return;
        		}
        	} else {
                content = new ByteArrayInputStream(parser.binaryValue());
                dataLength = content.available();
			}
        } else {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if ("content".equals(currentFieldName)) {
                        content = new ByteArrayInputStream(parser.binaryValue());
                        dataLength = content.available();
                    } else if ("_content_type".equals(currentFieldName)) {
                        contentType = parser.text();
                    } else if ("_name".equals(currentFieldName)) {
                        name = parser.text();
                    }
                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                    if ("_indexed_chars".equals(currentFieldName) || "_indexedChars".equals(currentFieldName)) {
                        indexedChars = parser.intValue();
                    }
                }
            }
        }
        // Throw clean exception when no content is provided Fix #23
        if (content == null) {
        	if (contentRefRoot != null && name != null) {
        		try {
                    File file = new File(contentRefRoot, name);
					content = new FileInputStream(file);
        			dataLength = file.length();
        		} catch (IOException e) {
                    if (!ignoreErrors) throw new MapperParsingException("Failed to resolve file path [" + contentRefRoot + "/" + name + "]", e);
                    if (logger.isDebugEnabled()) logger.debug("Ignoring IOException catch while parsing content: {}: {}", e.getMessage(), name);
                    return;
        		}
        	} else {
                throw new MapperParsingException("No content is provided.");
			}
        }

        Metadata metadata = new Metadata();
        if (contentType != null) {
            metadata.add(Metadata.CONTENT_TYPE, contentType);
        }
        if (name != null) {
            metadata.add(Metadata.RESOURCE_NAME_KEY, name);
        }
//        String parsedContent;
//        try {
//            parsedContent = tika().parseToString(content, metadata, indexedChars);
//        } catch (Throwable e) {
//            // #18: we could ignore errors when Tika does not parse data
//            if (!ignoreErrors) throw new MapperParsingException("Failed to extract text for [" + name + "]", e);
//            return;
//        }
		try {
			org.apache.tika.parser.ParseContext tikaContext = new org.apache.tika.parser.ParseContext();
			Parser tikaParser = new RecursiveMetadataParser(rootTikaParser, context, indexedChars, name);
			tikaContext.set(Parser.class, tikaParser);
			tikaParser.parse(content, new BodyContentHandler(), metadata, tikaContext);
		} catch (MapperParsingException e) {
			if (!ignoreErrors) throw e;
            if (logger.isDebugEnabled()) logger.debug("Ignoring MapperParsingException catch while parsing content: {}: {}", e.getMessage(), name);
			return;
		} catch (Throwable e) {
            if (!ignoreErrors) throw new MapperParsingException("Failed to extract text for [" + name + "]", e);
            if (logger.isDebugEnabled()) logger.debug("Ignoring Exception catch while parsing content: {}: {}", e.getMessage(), name);
            return;
		} finally {
			content.close();
		}
        //logger.info("parse OK");
//        context.externalValue(reusableContentData);
//        contentMapper.parse(context);

        try {
            context.externalValue(name);
            nameMapper.parse(context);
        } catch(MapperParsingException e){
            if (!ignoreErrors) throw e;
            if (logger.isDebugEnabled()) logger.debug("Ignoring MapperParsingException catch while parsing name: {}: {}", e.getMessage(), context.externalValue());
        }

        try {
            context.externalValue(metadata.get(Metadata.DATE));
            dateMapper.parse(context);
        } catch(MapperParsingException e){
            if (!ignoreErrors) throw e;
            if (logger.isDebugEnabled()) logger.debug("Ignoring MapperParsingException catch while parsing date: {}: {}", e.getMessage(), context.externalValue());
        }

        try {
            context.externalValue(metadata.get(Metadata.TITLE));
            titleMapper.parse(context);
        } catch(MapperParsingException e){
            if (!ignoreErrors) throw e;
            if (logger.isDebugEnabled()) logger.debug("Ignoring MapperParsingException catch while parsing title: {}: {}", e.getMessage(), context.externalValue());
        }

        try {
            context.externalValue(metadata.get(Metadata.AUTHOR));
            authorMapper.parse(context);
        } catch(MapperParsingException e){
            if (!ignoreErrors) throw e;
            if (logger.isDebugEnabled()) logger.debug("Ignoring MapperParsingException catch while parsing author: {}: {}", e.getMessage(), context.externalValue());
        }

        try {
            context.externalValue(metadata.get(Metadata.KEYWORDS));
            keywordsMapper.parse(context);
        } catch(MapperParsingException e){
            if (!ignoreErrors) throw e;
            if (logger.isDebugEnabled()) logger.debug("Ignoring MapperParsingException catch while parsing keywords: {}: {}", e.getMessage(), context.externalValue());
        }

        try {
            context.externalValue(metadata.get(Metadata.CONTENT_TYPE));
            contentTypeMapper.parse(context);
        } catch(MapperParsingException e){
            if (!ignoreErrors) throw e;
            if (logger.isDebugEnabled()) logger.debug("Ignoring MapperParsingException catch while parsing content_type: {}: {}", e.getMessage(), context.externalValue());
        }

        try {
            if (metadata.get(Metadata.CONTENT_LENGTH) != null) {
                // We try to get CONTENT_LENGTH from Tika first
                context.externalValue(metadata.get(Metadata.CONTENT_LENGTH));
            } else {
                // Otherwise, we use our byte[] length
                context.externalValue(dataLength);
            }
            contentLengthMapper.parse(context);
        } catch(MapperParsingException e){
            if (!ignoreErrors) throw e;
            if (logger.isDebugEnabled()) logger.debug("Ignoring MapperParsingException catch while parsing content_length: {}: {}", e.getMessage(), context.externalValue());
        }
    	sum_time += System.nanoTime() - stat_time;
    	docs_count++;
    	if (docs_count % 100 == 0) {
    		logger.debug("parse file field: [docs_count:{}, sum_time:{}, rate: {}", docs_count, sum_time, ((double)docs_count)/sum_time);
    	}
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
    	AttachmentMapper sourceMergeWith = (AttachmentMapper) mergeWith;
        if (!mergeContext.mergeFlags().simulate()) {
            if (sourceMergeWith.contentRefRoot != null) {
                this.contentRefRoot = sourceMergeWith.contentRefRoot;
            }
        }
    }

    @Override
    public void traverse(FieldMapperListener fieldMapperListener) {
        contentMapper.traverse(fieldMapperListener);
        dateMapper.traverse(fieldMapperListener);
        titleMapper.traverse(fieldMapperListener);
        nameMapper.traverse(fieldMapperListener);
        authorMapper.traverse(fieldMapperListener);
        keywordsMapper.traverse(fieldMapperListener);
        contentTypeMapper.traverse(fieldMapperListener);
        contentLengthMapper.traverse(fieldMapperListener);
    }

    @Override
    public void traverse(ObjectMapperListener objectMapperListener) {
    }

    @Override
    public void close() {
        contentMapper.close();
        dateMapper.close();
        titleMapper.close();
        nameMapper.close();
        authorMapper.close();
        keywordsMapper.close();
        contentTypeMapper.close();
        contentLengthMapper.close();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field("type", CONTENT_TYPE);
        builder.field("path", pathType.name().toLowerCase());
        builder.field("contentRefRoot", contentRefRoot);

        builder.startObject("fields");
        contentMapper.toXContent(builder, params);
        authorMapper.toXContent(builder, params);
        titleMapper.toXContent(builder, params);
        nameMapper.toXContent(builder, params);
        dateMapper.toXContent(builder, params);
        keywordsMapper.toXContent(builder, params);
        contentTypeMapper.toXContent(builder, params);
        contentLengthMapper.toXContent(builder, params);
        builder.endObject();

        builder.endObject();
        return builder;
    }
}
