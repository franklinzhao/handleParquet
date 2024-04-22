package parquetReader;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.Type;

import java.util.List;

public class ParquetDataWithSchema {
    private List<SimpleGroup> data;
    private List<Type> schema;

    public ParquetDataWithSchema(List<SimpleGroup> data, List<Type> schema) {
        this.data = data;
        this.schema = schema;
    }

    public List<SimpleGroup> getData() {
        return data;
    }

    public List<Type> getSchema() {
        return schema;
    }
}