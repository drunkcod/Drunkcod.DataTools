using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Drunkcod.DataTools
{
	public struct ResultColumn
	{
		public ResultColumn(string name, Type type) {
			this.Name = name;
			this.ColumnType = type;
		}

		public readonly string Name;
		public readonly Type ColumnType;
	}

	public class ResultSet<T> : IEnumerable<T>
	{
		public ResultColumn[] Columns;
		public List<T> Rows;

		IEnumerator<T> IEnumerable<T>.GetEnumerator() {
			return Rows.GetEnumerator();
		}

		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() {
			return Rows.GetEnumerator();
		}
	}

	public struct ProfileStatisticsRow
	{
		readonly ResultRow values;

		public ProfileStatisticsRow(ResultRow values)
		{
			this.values = values;
		}

		public long Rows { get {  return (long)values[0]; } }
		public long Executes { get { return (long)values[1]; } }
		public string StmtText { get { return AsString(values[2]); } }
		public int StmtId { get { return (int)values[3]; } }
		public int NodeId { get { return (int)values[4]; } }
		public int Parent { get { return (int)values[5]; } }
		public string PhysicalOp { get { return AsString(values[6]); } }
		public string LogicalOp  { get { return AsString(values[7]); } }
		public string Argument { get { return AsString(values[8]); } }
		public string DefinedValues { get { return AsString(values[9]); } }
		public float? EstimateRows { get { return Nullable<float>(values[10]); } }
		public float? EstimateIO { get { return Nullable<float>(values[11]); } }
		public float? EstimateCPU { get { return Nullable<float>(values[12]); } }
		public float? EstimateExecutions { get { return Nullable<float>(values[19]); } }
		public int? AvgRowSize { get { return Nullable<int>(values[13]); } }
		public float TotalSubtreeCost { get { return (float)values[14]; } }
		public string OutputList { get { return AsString(values[15]); } }
		public string Warnings { get { return AsString(values[16]); } }
		public string Type { get { return AsString(values[17]); } }
		public bool Parallell { get { return (bool)values[18]; } }

		static string AsString(object obj) { return obj as string ?? string.Empty; }
		static T? Nullable<T>(object obj) where T : struct {  return obj == DBNull.Value ? new T?() : (T)obj; }
	}

	public class ProfiledResultSet
	{
		public ResultSet<ProfileStatisticsRow> Statistics;
		public ResultSet<ResultRow> Result;
	}

	public struct ResultRow : IEnumerable<object>
	{
		readonly object[] values;

		public ResultRow(params object[] values) {
			this.values = values;
		}

		public static ResultRow From(SqlDataReader reader) {
			var values = new object[reader.FieldCount];
			reader.GetValues(values);
			return new ResultRow(values);
		}

		public int Count { get { return values.Length; } }

		public object this[int index]
		{
			get { return values[index]; }
			set { values[index] = value; }
		}

		IEnumerator<object> IEnumerable<object>.GetEnumerator() {
			return (values as IEnumerable<object>).GetEnumerator();
		}

		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() {
			return values.GetEnumerator();
		}
	}

	public class SqlDataCommand : IEnumerable<SqlParameter>
	{
		internal SqlCommand command;

		public SqlDataCommand(string sproc)
		{
			command = new SqlCommand(sproc) {
				CommandType = CommandType.StoredProcedure,
				CommandTimeout = 0,
			};
		}

		public void Add(string name, int value) {
			command.Parameters.Add(name, SqlDbType.Int).Value = value;
		}

		public void Add(string name, bool value) {
			command.Parameters.Add(name, SqlDbType.Bit).Value = value;
		}

		IEnumerator<SqlParameter> IEnumerable<SqlParameter>.GetEnumerator() {
			return command.Parameters.Cast<SqlParameter>().GetEnumerator();
		}

		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() {
			return command.Parameters.GetEnumerator();
		}
	}

	public class SqlDataConnection
	{
		readonly string connectionString;

		public SqlDataConnection(string connectionString) {
			this.connectionString = connectionString;
		}

		T WithCommand<T>(Func<SqlCommand, T> run) {
			using(var db = new SqlConnection(connectionString))
			using(var cmd = db.CreateCommand())
				return run(cmd);
		}

		public void ExecuteCommand(string command) {
			WithCommand(cmd => {
				cmd.CommandText = command;
				cmd.Connection.Open();
				cmd.ExecuteNonQuery();
				return true;
			});
		}

		public IEnumerable<ResultSet<ResultRow>> Query(string query) {
			return Query<ResultRow>(query);
		}

		public IEnumerable<ResultSet<T>> Query<T>(string query) {
			return WithCommand(cmd => {
				cmd.CommandText = query;
				cmd.Connection.Open();
				return ReadResultSet<T>(cmd).ToList();
			});
		}

		public IEnumerable<ProfiledResultSet> ProfileQuery(string query)
		{
			return WithCommand(cmd => {
				cmd.CommandText = "set statistics profile on";
				cmd.Connection.Open();
				cmd.ExecuteNonQuery();
				cmd.CommandText = query;
				return ReadProfiledResultSet(cmd).ToList();
			});			
		}

		public IEnumerable<ResultSet<ResultRow>> Exec(SqlDataCommand command) {
			using(var db = new SqlConnection(connectionString)) {
				var cmd = command.command;
				cmd.Connection = db;
				db.Open();
				return ReadResultSet<ResultRow>(cmd).ToList();
			}
		}

		public IEnumerable<ProfiledResultSet> ProfileExec(string sproc, params SqlParameter[] args) {
			return WithCommand(cmd => {
				cmd.CommandText = "set statistics profile on";
				cmd.Connection.Open();
				cmd.ExecuteNonQuery();
				cmd.CommandText = sproc;
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.CommandTimeout = 0;
				cmd.Parameters.AddRange(args);
				return ReadProfiledResultSet(cmd).ToList();
			});
		}

		IEnumerable<ResultSet<T>> ReadResultSet<T>(SqlCommand cmd) {
			using(var reader = cmd.ExecuteReader()) {
				do {
					var columns = GetResultColumns(reader);
					yield return new ResultSet<T> {
						Columns = columns,
						Rows = ReadResult(reader, (Converter<SqlDataReader,T>)GetConverter(typeof(T), columns)).ToList(),
					};
				} while(reader.NextResult());
			}
		}

		Delegate GetConverter(Type targetType, ResultColumn[] columns) {
			var converterType = typeof(Converter<,>).MakeGenericType(typeof(SqlDataReader), targetType);
			if(targetType == typeof(ResultRow))
				return Delegate.CreateDelegate(converterType, typeof(ResultRow).GetMethod("From"));

			var reader = Expression.Parameter(typeof(SqlDataReader));
			var ordinal = 0;
			var body = Expression.MemberInit(Expression.New(targetType), columns
				.Select(x => new {
					x.ColumnType,
					Ordinal = ordinal++,
					Member = targetType.GetMember(x.Name, BindingFlags.Instance | BindingFlags.Public | BindingFlags.SetField | BindingFlags.SetProperty).SingleOrDefault(),
				}).Where(x => x.Member != null)
				.Select(x => 
					Expression.Bind(x.Member,				
						Expression.Convert(
							Expression.Call(reader, typeof(SqlDataReader).GetMethod("Get" + x.ColumnType.Name), Expression.Constant(x.Ordinal))
						,GetMemberType(x.Member))
					)
				));

			var converter = Expression.Lambda(converterType, body, reader);
			
			return converter.Compile();
		}

		static Type GetMemberType(MemberInfo member) {
			var field = member as FieldInfo;
			if(field != null)
				return field.FieldType;
			return ((PropertyInfo)member).PropertyType;
		}

		static ResultColumn[] StatisticsColumns = new ResultColumn[] {
			new ResultColumn("Rows", typeof(Int64)),
			new ResultColumn("Executes", typeof(Int64)),
			new ResultColumn("StmtText", typeof(String)),
			new ResultColumn("StmtId",  typeof(Int32)),
			new ResultColumn("NodeId", typeof(Int32)),
			new ResultColumn("Parent", typeof(Int32)),
			new ResultColumn("PhysicalOp", typeof(String)),
			new ResultColumn("LogicalOp",  typeof(String)),
			new ResultColumn("Argument", typeof(String)),
			new ResultColumn("DefinedValues", typeof(String)),
			new ResultColumn("EstimateRows", typeof(Single)),
			new ResultColumn("EstimateIO", typeof(Single)),
			new ResultColumn("EstimateCPU", typeof(Single)),
			new ResultColumn("AvgRowSize",  typeof(Int32)),
			new ResultColumn("TotalSubtreeCost", typeof(Single)),
			new ResultColumn("OutputList", typeof(String)),
			new ResultColumn("Warnings", typeof(String)),
			new ResultColumn("Type", typeof(String)),
			new ResultColumn("Parallel", typeof(Boolean)),
			new ResultColumn("EstimateExecutions", typeof(Single))
		};

		IEnumerable<ProfiledResultSet> ReadProfiledResultSet(SqlCommand cmd) {
			ResultSet<ResultRow> result = null;
			using(var reader = cmd.ExecuteReader()) {
				do {
					var rows = ReadResult(reader, ResultRow.From);

					if(reader.FieldCount == StatisticsColumns.Length
					&& Enumerable.Range(0, reader.FieldCount).All(x => StatisticsColumns[x].Name == reader.GetName(x) && StatisticsColumns[x].ColumnType == reader.GetFieldType(x))) {
						yield return new ProfiledResultSet {
							Statistics = new ResultSet<ProfileStatisticsRow>
							{
								Columns = StatisticsColumns,
								Rows = rows.Select(x => new ProfileStatisticsRow(x)).ToList(),
							},
							Result = result,
						};
						result = null;
					}
					else {
						result = new ResultSet<ResultRow> {
							Columns = GetResultColumns(reader), 
							Rows = rows.ToList(),
						};
					}
				} while(reader.NextResult());
			}
		}

		ResultColumn[] GetResultColumns(SqlDataReader reader) {
			var columns = new ResultColumn[reader.FieldCount];
			for(var i = 0; i != columns.Length; ++i)
				columns[i] = new ResultColumn(reader.GetName(i), reader.GetFieldType(i)); 
			return columns;
		}

		IEnumerable<T> ReadResult<T>(SqlDataReader reader, Converter<SqlDataReader,T> materialize) {
			while(reader.Read())
				yield return materialize(reader);
		}
	}
}
