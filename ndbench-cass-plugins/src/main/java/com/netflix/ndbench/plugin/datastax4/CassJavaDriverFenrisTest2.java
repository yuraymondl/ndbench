package com.netflix.ndbench.plugin.datastax4;

import static com.netflix.ndbench.core.util.NdbUtil.humanReadableByteCount;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.data.ByteUtils;
import com.datastax.oss.driver.internal.core.DefaultConsistencyLevelRegistry;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;
import com.netflix.ndbench.core.config.IConfiguration;
import com.netflix.ndbench.core.util.CheckSumUtil;
import com.netflix.ndbench.plugin.QueryUtilFenris;
import com.netflix.ndbench.plugin.configs.CassandraFenris2Configuration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.lang.Math;

@Singleton
@NdBenchClientPlugin("CassJavaDriverFenrisTest2")
public class CassJavaDriverFenrisTest2 extends CJavaDriverBaseFenrisPlugin<CassandraFenris2Configuration> {

    @Inject
    protected CassJavaDriverFenrisTest2(
            CassJavaDriverManager cassJavaDriverManager, IConfiguration ndbConfig,
            CassandraFenris2Configuration cassandraFenris2Configuration) {
        super(cassJavaDriverManager, ndbConfig, cassandraFenris2Configuration);
    }

    @Override void prepStatements(CqlSession session) {
        //int nCols = config.getColsPerRow();
        int nCols = 10;
        String values = IntStream.range(0, nCols).mapToObj(i -> "value"+i).collect(Collectors.joining(", "));
        String bindValues = IntStream.range(0, nCols).mapToObj(i -> "?").collect(Collectors.joining(", "));
        writePstmt = session.prepare(String.format(QueryUtilFenris.INSERT_QUERY, keyspaceName, tableName, values, bindValues));
        writePstmt2 = session.prepare(String.format(QueryUtilFenris.INSERT_QUERY, keyspaceName, tableName2, values, bindValues));
        writePstmt3 = session.prepare(String.format(QueryUtilFenris.INSERT_QUERY, keyspaceName, tableName3, values, bindValues));
        writePstmt4 = session.prepare(String.format(QueryUtilFenris.INSERT_QUERY, keyspaceName, tableName4, values, bindValues));
        readPstmt = session.prepare(String.format(QueryUtilFenris.READ_QUERY, keyspaceName, tableName));
    }

    @Override void upsertKeyspace(CqlSession session) {
        upsertGenericKeyspace(session);
    }

    @Override void upsertCF(CqlSession session) {
        session.execute(QueryUtilFenris.upsertCFQuery(config.getColsPerRow(), keyspaceName, tableName));
        session.execute(QueryUtilFenris.upsertCFQuery(config.getColsPerRow(), keyspaceName, tableName2));
        session.execute(QueryUtilFenris.upsertCFQuery(config.getColsPerRow(), keyspaceName, tableName3));
        session.execute(QueryUtilFenris.upsertCFQuery(config.getColsPerRow(), keyspaceName, tableName4));
    }

    @Override public String readSingle(String key) throws Exception {
        int nRows = 0;

        BoundStatement bStmt = readPstmt.bind();
        bStmt.setString("key", key);
        bStmt.setConsistencyLevel(consistencyLevel(config.getReadConsistencyLevel()));
        ResultSet rs = session.execute(bStmt);
        List<Row> result=rs.all();

        if (!result.isEmpty())
        {
            nRows = result.size();
            if (config.getValidateRowsPerPartition() && nRows < (config.getRowsPerPartition()))
            {
                throw new Exception("Num rows returned not ok " + nRows);
            }

            if (ndbConfig.isValidateChecksum())
            {
                for (Row row : result)
                {
                    for (int i = 0; i < config.getColsPerRow(); i++)
                    {
                        String value = ByteUtils.toHexString(row.getByteBuffer(getValueColumnName(i)));
                        if (!CheckSumUtil.isChecksumValid(value))
                        {
                            throw new Exception(String.format("Value %s is corrupt. Key %s.", value, key));
                        }
                    }
                }
            }
        }
        else {
            return CacheMiss;
        }

        return ResultOK;
    }

    private String getValueColumnName(int index)
    {
        return "value" + index;
    }

    private ConsistencyLevel consistencyLevel(String consistencyLevelConfig) {
        int code = new DefaultConsistencyLevelRegistry().nameToCode(consistencyLevelConfig);
        return DefaultConsistencyLevel.fromCode(code);
    }

    @Override public String writeSingle(String key) {
        BatchStatementBuilder builder = BatchStatement
                .builder(BatchType.LOGGED)
                .setConsistencyLevel(consistencyLevel(config.getWriteConsistencyLevel()));

	double rand = Math.random();
	BoundStatement bStmt;
	BoundStatement bStmt2;
	BoundStatement bStmt3;
	BoundStatement bStmt4;
	if (rand >= 0.05) {
	    //95%
            bStmt = writePstmt.bind()
                    .setString("key", key)
                    .setByteBuffer("value0", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value1", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value2", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value3", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value4", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()));
            bStmt2 = writePstmt2.bind()
                    .setString("key", key)
                    .setByteBuffer("value0", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value1", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value2", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value3", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value4", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()));
            bStmt3 = writePstmt3.bind()
                    .setString("key", key)
                    .setByteBuffer("value0", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value1", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value2", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value3", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value4", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()));
            bStmt4 = writePstmt4.bind()
                    .setString("key", key)
                    .setByteBuffer("value0", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value1", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value2", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value3", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value4", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()));
	} else {
	    //5%
            bStmt = writePstmt.bind()
                    .setString("key", key)
                    .setByteBuffer("value5", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value6", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value7", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value8", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value9", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()));
            bStmt2 = writePstmt2.bind()
                    .setString("key", key)
                    .setByteBuffer("value5", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value6", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value7", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value8", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value9", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()));
            bStmt3 = writePstmt3.bind()
                    .setString("key", key)
                    .setByteBuffer("value5", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value6", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value7", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value8", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value9", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()));
            bStmt4 = writePstmt4.bind()
                    .setString("key", key)
                    .setByteBuffer("value5", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value6", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value7", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value8", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()))
                    .setByteBuffer("value9", ByteUtils.fromHexString("0x" + this.dataGenerator.getRandomValue() + this.dataGenerator.getRandomValue()));
        }

        builder.addStatement(bStmt);
        builder.addStatement(bStmt2);
        builder.addStatement(bStmt3);
        builder.addStatement(bStmt4);

        session.execute(builder.build());
        return ResultOK;
    }

    @Override public void shutdown() throws Exception {
        session.close();
    }

    @Override public String getConnectionInfo() {
        int bytesPerCol=ndbConfig.getDataSize();
        int numColsPerRow=config.getColsPerRow();
        int numRowsPerPartition=config.getRowsPerPartition();
        int numPartitions= ndbConfig.getNumKeys();
        int RF = 3;
        Long numNodes = session.getMetadata().getNodes().values()
                .stream()
                .collect(groupingBy(Node::getDatacenter,counting()))
                .values().stream().findFirst().get();


        int partitionSizeInBytes = bytesPerCol * numColsPerRow * numRowsPerPartition;
        long totalSizeInBytes = (long) partitionSizeInBytes * numPartitions * RF;
        long totalSizeInBytesPerNode = totalSizeInBytes / numNodes;



        return String.format("Cluster Name - %s : Keyspace Name - %s : CF Names - %s, %s, %s, %s ::: ReadCL - %s : WriteCL - %s ::: " +
                        "DataSize per Node: ~[%s], Total DataSize on Cluster: ~[%s], Num nodes in C* DC: %s, PartitionSize: %s",
                sessionName, keyspaceName, tableName, tableName2, tableName3, tableName4, config.getReadConsistencyLevel(), config.getWriteConsistencyLevel(),
                humanReadableByteCount(totalSizeInBytesPerNode),
                humanReadableByteCount(totalSizeInBytes),
                numNodes,
                humanReadableByteCount(partitionSizeInBytes));
    }

    private SimpleStatement SimpleStatement(String query) {
        return SimpleStatement.builder(query).build();
    }
}
