using Cone;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Drunkcod.DataTools.Specs
{
	[Describe(typeof(ResultSetComparer))]
	public class ResultSetComparerSpec
	{
		public void DBNull_is_sorted_first() {
			Check.That(
				() => ResultSetComparer.CompareRows(new ResultRow(DBNull.Value), new ResultRow(1)) < 0,
				() => ResultSetComparer.CompareRows(new ResultRow(1), new ResultRow(DBNull.Value)) > 0);
		}

		public void int_is_sorted_ascending() {
			Check.That(
				() => ResultSetComparer.CompareRows(new ResultRow(1), new ResultRow(2)) < 0,
				() => ResultSetComparer.CompareRows(new ResultRow(2), new ResultRow(1)) > 0);
		}
	}
}
