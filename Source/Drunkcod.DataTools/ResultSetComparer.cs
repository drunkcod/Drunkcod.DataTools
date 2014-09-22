using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Drunkcod.DataTools
{
	public class ResultSetComparer
	{
		public Action<int, ResultRow, ResultRow> OnRowMismatch;
		public void CompareResults(ResultSet<ResultRow> x, ResultSet<ResultRow> y, bool ignoreOrdering) {
			if(x.Columns.Length != y.Columns.Length)
				throw new InvalidOperationException("Different number of columns");
			if(x.Rows.Count != y.Rows.Count)
				throw new InvalidOperationException("Different number of rows");

			if (ignoreOrdering)
			{
				x.Rows.Sort(CompareRows);
				y.Rows.Sort(CompareRows);
			}
		
			for(var i = 0; i != x.Rows.Count; ++i) {
				if(CompareRows(x.Rows[i], y.Rows[i]) != 0)
					OnRowMismatch(i, x.Rows[i], y.Rows[i]);
			}
		}

		public static int CompareRows(ResultRow a, ResultRow b) {
			for(var i = 0; i != a.Count; ++i) {					
				var x = a[i];
				var y = b[i];

				var yNull = ReferenceEquals(y, DBNull.Value);
				if(ReferenceEquals(x, DBNull.Value)) {
					if(yNull)
						continue;
					return -1;
				}
				else if(yNull)
					return 1;

				var r = (x as IComparable).CompareTo(y);
				if(r != 0)
					return r;
			}
			return 0;
		}
	}
}
