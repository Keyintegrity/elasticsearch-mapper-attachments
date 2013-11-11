package org.elasticsearch.index.mapper.attachment;


public final class MapperBuilders {

    private MapperBuilders() {

    }
    
    public static BigStringFieldMapper.Builder bigStringField(String name) {
        return new BigStringFieldMapper.Builder(name);
    }

}