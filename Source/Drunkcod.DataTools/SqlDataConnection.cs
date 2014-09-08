using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

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

	public class ResultSet<T>
	{
		public ResultColumn[] Columns;
		public List<T> Rows;
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

		ResultRow(object[] values) {
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
		}

		IEnumerator<object> IEnumerable<object>.GetEnumerator() {
			return (values as IEnumerable<object>).GetEnumerator();
		}

		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() {
			return values.GetEnumerator();
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
			return WithCommand(cmd => {
				cmd.CommandText = query;
				cmd.Connection.Open();
				return ReadResultSet(cmd).ToList();
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

		public IEnumerable<ResultSet<ResultRow>> Exec(string sproc, params SqlParameter[] args) {
			return WithCommand(cmd => {
				cmd.CommandText = sproc;
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.CommandTimeout = 0;
				cmd.Parameters.AddRange(args);
				cmd.Connection.Open();
				return ReadResultSet(cmd).ToList();
			});
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

		IEnumerable<ResultSet<ResultRow>> ReadResultSet(SqlCommand cmd) {
			using(var reader = cmd.ExecuteReader()) {
				do {
					yield return new ResultSet<ResultRow> {
						Columns = Enumerable.Range(0, reader.FieldCount)
						.Select(x => new ResultColumn(reader.GetName(x), reader.GetFieldType(x))).ToArray(),
						Rows = ReadResult(reader).ToList(),
					};
				} while(reader.NextResult());
			}
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
					var rows = ReadResult(reader);

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
							Columns = Enumerable.Range(0, reader.FieldCount)
							.Select(x => new ResultColumn(reader.GetName(x), reader.GetFieldType(x))).ToArray(),
							Rows = rows.ToList(),
						};
					}
				} while(reader.NextResult());
			}
		}


		IEnumerable<ResultRow> ReadResult(SqlDataReader reader) {
			while(reader.Read())
				yield return ResultRow.From(reader);
		}
	}
}
