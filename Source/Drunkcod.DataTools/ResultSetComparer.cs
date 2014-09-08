using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Drunkcod.DataTools
{
	public class ResultSetComparer
	{
		public Action<ResultRow, ResultRow> OnRowMismatch;
		public void CompareResults(ResultSet<ResultRow> x, ResultSet<ResultRow> y, bool ignoreOrdering) {
			if(x.Columns.Length != y.Columns.Length)
				throw new InvalidOperationException("Different number of columns");
			if(x.Rows.Count != y.Rows.Count)
				throw new InvalidOperationException("Different number of rows");
	
			if(ignoreOrdering) {
				x.Rows.Sort(CompareRows);
				y.Rows.Sort(CompareRows);
			}
		
			for(var j = 0; j != x.Rows.Count; ++j) {
				if(CompareRows(x.Rows[j], y.Rows[j]) != 0)
					OnRowMismatch(x.Rows[j], y.Rows[j]);
			}
		}

		static int CompareRows(ResultRow a, ResultRow b) {
			for(var i = 0; i != a.Count; ++i) {
				var r = (a[i] as IComparable).CompareTo(b[i]);
				if(r != 0)
					return r;
			}
			return 0;
		}
	}
}
