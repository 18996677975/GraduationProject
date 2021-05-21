from pyflink.datastream import StreamExecutionEnvironment, DataStream
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.descriptors import Schema, OldCsv, FileSystem

env = StreamExecutionEnvironment.get_execution_environment()
fileDs = env.read_text_file('/home/cy/PycharmProjects/毕业设计/ETL/ETL-Flink/input1')

a = fileDs.map(lambda x: int(x) + 50)

a.print()

env.execute('test')

# t_env.connect(FileSystem().path('/home/cy/PycharmProjects/毕业设计/ETL/ETL-Flink/input1')) \
#     .with_format(OldCsv().field('word', DataTypes.STRING())) \
#     .with_schema(Schema().field('word', DataTypes.STRING())) \
#     .create_temporary_table('mySource')
#
# t_env.connect(FileSystem().path('/home/cy/PycharmProjects/毕业设计/ETL/ETL-Flink/output2')) \
#     .with_format(OldCsv().field_delimiter('\t').field('word', DataTypes.STRING()).field('count', DataTypes.BIGINT())) \
#     .with_schema(Schema().field('word', DataTypes.STRING()).field('count', DataTypes.BIGINT())) \
#     .create_temporary_table('mySink')
#
# t_env.from_path('mySource') \
#     .group_by('word') \
#     .select('word, count(1)') \
#     .insert_into('mySink')
#
# t_env.execute('WordCount')