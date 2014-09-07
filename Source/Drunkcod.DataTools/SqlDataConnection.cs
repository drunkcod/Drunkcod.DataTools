using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace Drunkcod.DataTools
{
	public struct ResultColumn
	{
		public string Name;
		public Type ColumnType;
	}

	public class ResultSet
	{
		public ResultColumn[] Columns;
		public List<ResultRow> Rows;
	}

	public struct ResultRow
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

		public IEnumerable<ResultSet> Query(string query) {
			return WithCommand(cmd => {
				cmd.CommandText = query;
				cmd.Connection.Open();
				return ReadResultSet(cmd).ToList();
			});
		}

		public IEnumerable<ResultSet> Exec(string sproc, params SqlParameter[] args) {
			return WithCommand(cmd => {
				cmd.CommandText = sproc;
				cmd.CommandType = CommandType.StoredProcedure;
				cmd.CommandTimeout = 0;
				cmd.Parameters.AddRange(args);
				cmd.Connection.Open();
				return ReadResultSet(cmd).ToList();
			});
		}

		IEnumerable<ResultSet> ReadResultSet(SqlCommand cmd) {
			using(var reader = cmd.ExecuteReader()) {
				do {
					yield return new ResultSet {
						Columns = Enumerable.Range(0, reader.FieldCount)
						.Select(x => new ResultColumn {
							Name = reader.GetName(x),
							ColumnType = reader.GetFieldType(x),
						}).ToArray(),
						Rows = ReadResult(reader)
					};
				} while(reader.NextResult());
			}
		}

		List<ResultRow> ReadResult(SqlDataReader reader) {
			var result = new List<ResultRow>();
			while(reader.Read())
				result.Add(ResultRow.From(reader));

			return result;
		}
	}
}
