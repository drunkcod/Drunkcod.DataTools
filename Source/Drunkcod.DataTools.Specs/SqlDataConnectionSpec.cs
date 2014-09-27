using Cone;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Drunkcod.DataTools.Specs
{
	[Describe(typeof(SqlDataConnection))]
	public class SqlDataConnectionSpec
	{
		SqlDataConnection db = new SqlDataConnection("Server=.;Integrated Security=SSPI");

		public class FooBarRow
		{
			public int Foo;
			public string Bar { get; set; }
		}

		public void typed_query_supports_props_and_fields() {
			var result = db.Query<FooBarRow>("select Foo = 42, Bar = 'Hello World'").ToList();

			Check.That(() => result.Count == 1);
			Check.That(() => result[0].Rows.Count == 1);
			Check.That(
				() => result[0].Rows[0].Foo == 42,
				() => result[0].Rows[0].Bar == "Hello World");
		}

		public void typed_query_coerce_long_to_int() {
			var result = db.Query<FooBarRow>("select Foo = cast(13 as bigint)").ToList();

			Check.That(() => result.Count == 1);
			Check.That(() => result[0].Rows.Count == 1);
			Check.That(() => result[0].Rows[0].Foo == 13);
		
		}
	}
}
