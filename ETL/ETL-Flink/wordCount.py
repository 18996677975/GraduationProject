from pyflink.dataset import ExecutionEnvironment
from pyflink.table import TableConfig, BatchTableEnvironment, DataTypes
from pyflink.table.descriptors import Schema, OldCsv, FileSystem

exec_env = ExecutionEnvironment.get_execution_environment()
exec_env.set_parallelism(1)
t_config = TableConfig()
t_env = BatchTableEnvironment.create(exec_env, t_config)

t_env.connect(FileSystem().path('/home/cy/PycharmProjects/GraduationProject/ETL/ETL-Flink/input')) \
    .with_format(OldCsv().field('word', DataTypes.STRING())) \
    .with_schema(Schema().field('word', DataTypes.STRING())) \
    .create_temporary_table('mySource')

t_env.connect(FileSystem().path('/home/cy/PycharmProjects/GraduationProject/ETL/ETL-Flink/output')) \
    .with_format(OldCsv().field_delimiter('\t').field('word', DataTypes.STRING()).field('count', DataTypes.BIGINT())) \
    .with_schema(Schema().field('word', DataTypes.STRING()).field('count', DataTypes.BIGINT())) \
    .create_temporary_table('mySink')

t_env.from_path('mySource') \
    .group_by('word') \
    .select('word, count(1)') \
    .insert_into('mySink')

t_env.execute('WordCount')