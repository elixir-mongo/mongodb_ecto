# defmodule Mongo.Ecto.NormalizedQueryTest do
#   use ExUnit.Case, async: true

#   alias Mongo.Ecto.NormalizedQuery

#   alias Ecto.Queryable
#   import Ecto.Query

#   defmodule Schema do
#     use Ecto.Schema

#     schema "schema" do
#       field :x, :integer
#       field :y, :integer, default: 5
#       field :z, {:array, :integer}
#     end
#   end

#   @id_types %{binary_id: Mongo.Ecto.ObjectID}

#   defp normalize(query, operation \\ :all) do
#     {query, params, _key} = Ecto.Query.Planner.prepare(query, operation, Mongo.Ecto)
#     query = Ecto.Query.Planner.normalize(query, operation, Mongo.Ecto)
#     IO.inspect query
#     apply(NormalizedQuery, operation, [query, params])
#   end

#   defp insert(schema, values) do
#     source = schema.__schema__(:source)
#     [pk] = schema.__schema__(:primary_key)

#     NormalizedQuery.insert(%{source: {source, nil}, schema: schema}, values, pk)
#   end

#   defmacrop assert_query(query, kw) do
#     Enum.map(kw, fn {key, value} ->
#       quote do
#         assert unquote(query).unquote(key) == unquote(value)
#       end
#     end)
#   end

#   test "bare schema" do
#     query = Schema |> from |> normalize
#     assert_query(query, coll: "schema", query: %{},
#                  projection: %{_id: true, x: true, y: true, z: true},
#                  opts: [])
#     assert [{:&, _, _}] = query.fields
#   end

#   test "from without schema" do
#     query = "posts" |> select([r], r.x) |> normalize
#     assert_query(query, coll: "posts", projection: %{x: true})
#     assert [{:field, :x, _}] = query.fields

#     query = "posts" |> select([r], {r, r.x}) |> normalize
#     assert_query(query, coll: "posts", projection: %{})
#     assert [{:&, _, _}, {:field, :x, _}] = query.fields
#   end

#   test "where" do
#     query = Schema |> where([r], r.x == 42) |> where([r], r.y != 43)
#                   |> select([r], r.x) |> normalize
#     assert_query(query, query: %{y: ["$ne": 43], x: 42}, projection: %{x: true})

#     query = Schema |> where([r], r.x > 5) |> where([r], r.x < 10) |> normalize
#     assert_query(query, query: %{x: ["$gt": 5, "$lt": 10]})

#     query = Schema |> where([r], not (r.x == 42)) |> normalize
#     assert_query(query, query: %{x: ["$ne": 42]})
#   end

#   test "select" do
#     query = Schema |> select([r], {r.x, r.y}) |> normalize
#     assert_query(query, projection: %{y: true, x: true})
#     assert [{:field, :x, _}, {:field, :y, _}] = query.fields

#     query = Schema |> select([r], [r.x, r.y]) |> normalize
#     assert_query(query, projection: %{y: true, x: true})
#     assert [{:field, :x, _}, {:field, :y, _}] = query.fields

#     query = Schema |> select([r], [r, r.x]) |> normalize
#     assert_query(query, projection: %{_id: true, x: true, y: true, z: true})
#     assert [{:&, _, _}, {:field, :x, _}] = query.fields

#     query = Schema |> select([r], [r]) |> normalize
#     assert_query(query, projection: %{_id: true, x: true, y: true, z: true})
#     assert [{:&, _, _}] = query.fields

#     query = Schema |> select([r], {1}) |> normalize
#     assert_query(query, projection: %{},
#                         fields: [{:value, 1, 1}])

#     query = Schema |> select([r], [r.id]) |> normalize
#     assert_query(query, projection: %{_id: true})
#     assert [{:field, :id, _}] = query.fields

#     query = from(r in Schema) |> normalize
#     assert_query(query, projection: %{_id: true, x: true, y: true, z: true})
#     assert [{:&, _, _}] = query.fields
#   end

#   test "count" do
#     query = Schema |> select([r], count(r.id)) |> normalize
#     assert_query(query, coll: "schema", query: %{})

#     query = Schema |> select([r], count(r.id)) |> where([r], r.x > 10) |> normalize
#     assert_query(query, coll: "schema", query: %{x: ["$gt": 10]})

#     assert_raise Ecto.QueryError, fn ->
#       Schema |> select([r], {r.id, count(r.id)}) |> normalize
#     end

#     assert_raise Ecto.QueryError, fn ->
#       Schema |> select([r], {count(r.id), r.id}) |> normalize
#     end
#   end

#   test "max" do
#     group_stage = ["$group": [_id: nil, value: [{"$max", "$x"}]]]
#     query = Schema |> select([r], max(r.x)) |> normalize
#     assert_query(query, coll: "schema",
#                  pipeline: [group_stage])

#     query = Schema |> select([r], max(r.x)) |> where([r], r.x == 10) |> normalize
#     assert_query(query, coll: "schema",
#                  pipeline: [["$match": %{"x": 10}], group_stage])

#     query = Schema |> select([r], max(r.x)) |> limit([r], 3) |> offset([r], 5) |> normalize
#     assert_query(query, coll: "schema",
#                  pipeline: [["$limit": 3], ["$skip": 5], group_stage])

#     assert_raise Ecto.QueryError, fn ->
#       Schema |> select([r], {max(r.x), r.id}) |> normalize
#     end
#   end

#   test "min" do
#     group_stage = ["$group": [_id: nil, value: [{"$min", "$x"}]]]
#     query = Schema |> select([r], min(r.x)) |> normalize
#     assert_query(query, coll: "schema",
#                  pipeline: [group_stage])

#     query = Schema |> select([r], min(r.x)) |> where([r], r.x == 10) |> normalize
#     assert_query(query, coll: "schema",
#                  pipeline: [["$match": %{"x": 10}], group_stage])

#     query = Schema |> select([r], min(r.x)) |> limit([r], 3) |> offset([r], 5) |> normalize
#     assert_query(query, coll: "schema",
#                  pipeline: [["$limit": 3], ["$skip": 5], group_stage])

#     assert_raise Ecto.QueryError, fn ->
#       Schema |> select([r], {min(r.x), r.id}) |> normalize
#     end
#   end

#   test "sum" do
#     group_stage = ["$group": [_id: nil, value: [{"$sum", "$x"}]]]
#     query = Schema |> select([r], sum(r.x)) |> normalize
#     assert_query(query, coll: "schema",
#                  pipeline: [group_stage])

#     query = Schema |> select([r], sum(r.x)) |> where([r], r.x == 10) |> normalize
#     assert_query(query, coll: "schema",
#                  pipeline: [["$match": %{"x": 10}], group_stage])

#     query = Schema |> select([r], sum(r.x)) |> limit([r], 3) |> offset([r], 5) |> normalize
#     assert_query(query, coll: "schema",
#                  pipeline: [["$limit": 3], ["$skip": 5], group_stage])

#     assert_raise Ecto.QueryError, fn ->
#       Schema |> select([r], {sum(r.x), r.id}) |> normalize
#     end
#   end

#   test "avg" do
#     group_stage = ["$group": [_id: nil, value: [{"$avg", "$x"}]]]
#     query = Schema |> select([r], avg(r.x)) |> normalize
#     assert_query(query, coll: "schema",
#                  pipeline: [group_stage])

#     query = Schema |> select([r], avg(r.x)) |> where([r], r.x == 10) |> normalize
#     assert_query(query, coll: "schema",
#                  pipeline: [["$match": %{"x": 10}], group_stage])

#     query = Schema |> select([r], avg(r.x)) |> limit([r], 3) |> offset([r], 5) |> normalize
#     assert_query(query, coll: "schema",
#                  pipeline: [["$limit": 3], ["$skip": 5], group_stage])

#     assert_raise Ecto.QueryError, fn ->
#       Schema |> select([r], {avg(r.x), r.id}) |> normalize
#     end
#   end

#   test "order by" do
#     query = Schema |> order_by([r], r.x) |> select([r], r.x) |> normalize
#     assert_query(query, query: %{}, order: [x: 1])

#     query = Schema |> order_by([r], [r.x, r.y]) |> select([r], r.x) |> normalize
#     assert_query(query, query: %{}, order: [x: 1, y: 1])

#     query = Schema |> order_by([r], [asc: r.x, desc: r.y]) |> select([r], r.x) |> normalize
#     assert_query(query, query: %{}, order: [x: 1, y: -1])

#     query = Schema |> order_by([r], []) |> select([r], r.x) |> normalize
#     assert_query(query, query: %{}, order: %{})
#   end

#   test "limit and offset" do
#     query = Schema |> limit([r], 3) |> normalize
#     assert_query(query, opts: [limit: 3])

#     query = Schema |> offset([r], 5) |> normalize
#     assert_query(query, opts: [skip: 5])

#     query = Schema |> offset([r], 5) |> limit([r], 3) |> normalize
#     assert_query(query, opts: [limit: 3, skip: 5])
#   end

#   test "lock" do
#     assert_raise Ecto.QueryError, fn ->
#       Schema |> lock("FOR SHARE NOWAIT") |> normalize
#     end
#   end

#   test "sql fragments" do
#     assert_raise Ecto.QueryError, fn ->
#       Schema |> select([r], fragment("downcase(?)", r.x)) |> normalize
#     end
#   end

#   test "fragments in where" do
#     query = Schema |> where([], fragment(x: 1)) |> normalize
#     assert_query(query, query: %{x: 1})

#     query = Schema |> where([], fragment(x: ["$in": ^[1, 2, 3]])) |> normalize
#     assert_query(query, query: %{x: ["$in": [1, 2, 3]]})

#     query = Schema |> where([], fragment(^[x: 1])) |> normalize
#     assert_query(query, query: %{x: 1})
#   end

#   test "fragments in select" do
#     query = Schema |> select([], fragment("z.$": 1)) |> normalize
#     assert_query(query, projection: %{"z.$": 1})
#     assert [{:fragment, _, _}] = query.fields

#     query = Schema |> select([r], {r.x, fragment("z.$": 1)}) |> normalize
#     assert_query(query, projection: %{"z.$": 1, x: true})
#     assert [{:field, :x, _}, {:fragment, _, _}] = query.fields
#   end

#   test "distinct" do
#     assert_raise Ecto.QueryError, fn ->
#       Schema |> distinct([r], r.x) |> select([r], {r.x, r.y}) |> normalize
#     end

#     assert_raise Ecto.QueryError, fn ->
#       Schema |> distinct(true) |> select([r], {r.x, r.y}) |> normalize
#     end
#   end

#   test "is_nil" do
#     query = Schema |> where([r], is_nil(r.x)) |> normalize
#     assert_query(query, query: %{x: nil})

#     query = Schema |> where([r], not is_nil(r.x)) |> normalize
#     assert_query(query, query: %{x: ["$ne": nil]})
#   end

#   test "tagged values in queries" do
#     query = Schema |> select([r], type(^"1", :integer)) |> normalize
#     assert [{:value, 1, _}] = query.fields
#   end

#   test "literals" do
#     query = Schema |> select([], nil) |> normalize
#     assert [{:value, nil, nil}] = query.fields

#     query = "plain" |> select([r], r.x) |> where([r], r.x == true) |> normalize
#     assert_query(query, query: %{x: true})

#     query = "plain" |> select([r], r.x) |> where([r], r.x == false) |> normalize
#     assert_query(query, query: %{x: false})

#     query = "plain" |> select([r], r.x) |> where([r], r.x == "abc") |> normalize
#     assert_query(query, query: %{x: "abc"})

#     query = "plain" |> select([r], r.x) |> where([r], r.x == 123) |> normalize
#     assert_query(query, query: %{x: 123})

#     query = "plain" |> select([r], r.x) |> where([r], r.x == 123.0) |> normalize
#     assert_query(query, query: %{x: 123.0})
#   end


#   test "in expression" do
#     query = Schema |> where([e], e.x in []) |> normalize
#     assert_query(query, query: %{x: ["$in": []]})

#     query = Schema |> where([e], e.x in ^[1, 2, 3]) |> normalize
#     assert_query(query, query: %{x: ["$in": [1, 2, 3]]})

#     query = Schema |> where([e], e.x in [1, ^2, 3]) |> normalize
#     assert_query(query, query: %{x: ["$in": [1, 2, 3]]})

#     query = Schema |> where([e], 1 in e.z) |> normalize
#     assert_query(query, query: %{z: 1})

#     assert_raise Ecto.QueryError, fn ->
#       Schema |> where([e], 1 in ^[]) |> normalize
#     end

#     assert_raise Ecto.QueryError, fn ->
#       Schema |> where([e], e.x in [1, e.x, 3]) |> normalize
#     end
#   end



#   # *_all

#   test "update all" do
#     query = from(m in Schema, update: [set: [x: 0]]) |> normalize(:update_all)
#     assert_query(query, command: %{"$set": [x: 0]},
#                         query: %{})

#     query = from(m in Schema, update: [set: [x: 0], inc: [y: 1, z: [-3]]]) |> normalize(:update_all)
#     assert_query(query, command: %{"$set": [x: 0], "$inc": [y: 1, z: [-3]]})

#     query = from(e in Schema, where: e.x == 123, update: [set: [x: 0]]) |> normalize(:update_all)
#     assert_query(query, command: %{"$set": [x: 0]},
#                         query: %{x: 123})

#     query = from(m in Schema, update: [set: [x: 0, y: ^"123"]]) |> normalize(:update_all)
#     assert_query(query, command: %{"$set": [x: 0, y: 123]})

#     query = from(m in Schema, update: [set: [x: ^0]]) |> normalize(:update_all)
#     assert_query(query, command: %{"$set": [x: 0]})

#     query = from(m in Schema, update: [set: [x: 0]], update: [set: [y: 123]]) |> normalize(:update_all)
#     assert_query(query, command: %{"$set": [x: 0, y: 123]})

#     assert_raise Ecto.QueryError, fn ->
#       from(m in Schema, limit: 5, update: [set: [x: 0]]) |> normalize(:update_all)
#     end

#     assert_raise Ecto.QueryError, fn ->
#       from(m in Schema, offset: 5, update: [set: [x: 0]]) |> normalize(:update_all)
#     end
#   end

#   test "update all with array ops" do
#     query = from(m in Schema, update: [push: [z: 0]]) |> normalize(:update_all)
#     assert_query(query, command: %{"$push": [z: 0]})

#     query = from(m in Schema, update: [pull: [z: 0]]) |> normalize(:update_all)
#     assert_query(query, command: %{"$pull": [z: 0]})
#   end

#   test "delete all" do
#     query = Schema |> Queryable.to_query |> normalize(:delete_all)
#     assert_query(query, query: %{})

#     query = from(e in Schema, where: e.x == 123) |> normalize(:delete_all)
#     assert_query(query, query: %{x: 123})

#     assert_raise Ecto.QueryError, fn ->
#       from(m in Schema, limit: 5) |> normalize(:delete_all)
#     end

#     assert_raise Ecto.QueryError, fn ->
#       from(m in Schema, offset: 5) |> normalize(:delete_all)
#     end
#   end

#   test "insert skips default nil values" do
#     query = Schema |> insert(x: nil, y: nil, z: nil)
#     assert_query(query, command: [y: nil, z: nil])

#     query = Schema |> insert(x: 1, y: 5, z: [])
#     assert_query(query, command: [x: 1, y: 5, z: []])
#   end
# end
