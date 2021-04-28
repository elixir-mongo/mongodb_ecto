# 0/26 passed
defmodule Mongo.Ecto.NormalizedQueryNewTest do
  use ExUnit.Case, async: true

  import Ecto.Query

  alias Ecto.Queryable

  defmodule Schema do
    use Ecto.Schema

    schema "schema" do
      field :x, :integer
      field :y, :integer
      field :z, :integer
      field :w, {:array, :integer}

      has_many :comments, Mongo.Ecto.NormalizedQueryNewTest.Schema2,
        references: :x,
        foreign_key: :z

      has_one :permalink, Mongo.Ecto.NormalizedQueryNewTest.Schema3,
        references: :y,
        foreign_key: :id
    end
  end

  defmodule Schema2 do
    use Ecto.Schema

    schema "schema2" do
      belongs_to :post, Mongo.Ecto.NormalizedQueryNewTest.Schema,
        references: :x,
        foreign_key: :z
    end
  end

  defmodule Schema3 do
    use Ecto.Schema

    schema "schema3" do
      field :list1, {:array, :string}
      field :list2, {:array, :integer}
      field :binary, :binary
    end
  end

  defp normalize(query, operation \\ :all) do
    {query, params, _key} = Ecto.Query.Planner.plan(query, operation, Mongo.Ecto)
    query = Ecto.Query.Planner.normalize(query, operation, Mongo.Ecto, 0)
    apply(Mongo.Ecto.NormalizedQuery, operation, [query, params])
  end

  defmacrop assert_fields(map, kw) do
    Enum.map(kw, fn {key, value} ->
      quote do
        assert unquote(map).unquote(key) == unquote(value)
      end
    end)
  end

  test "bare schema" do
    query = Schema |> from |> normalize

    assert_fields query,
      coll: "schema",
      query: %{},
      projection: %{},
      opts: [],
      fields: []
  end

  test "from" do
    query = Schema |> select([r], r.x) |> normalize

    assert_fields query,
      coll: "schema",
      query: %{},
      projection: %{x: true},
      opts: []

    assert [{:field, :x, _}] = query.fields
  end

  test "from without schema" do
    query = "posts" |> select([r], r.x) |> normalize

    assert_fields query,
      coll: "posts",
      projection: %{x: true}

    assert [{:field, :x, _}] = query.fields

    query = "posts" |> select([:x]) |> normalize

    assert_fields query,
      coll: "posts",
      projection: %{x: true}

    assert [{:field, _, _}] = query.fields

    # query = from(p in "posts", select: p) |> normalize()

    # assert_fields query,
    #   coll: "posts",
    #   projection: %{}

    # assert [{:&, _, _}] = query.fields
  end

  test "from with subquery" do
    assert_raise ArgumentError, "MongoDB does not support subqueries", fn ->
      subquery("posts" |> select([r], r.x)) |> select([r], r.x) |> normalize
    end
  end

  test "select" do
    query = Schema |> select([r], {r.x, r.y}) |> normalize
    assert_fields query, projection: %{y: true, x: true}
    assert [{:field, :x, _}, {:field, :y, _}] = query.fields

    query = Schema |> select([r], [r.x, r.y]) |> normalize
    assert_fields query, projection: %{y: true, x: true}
    assert [{:field, :x, _}, {:field, :y, _}] = query.fields

    query = Schema |> select([r], struct(r, [:x, :y])) |> normalize
    assert_fields query, projection: %{y: true, x: true}
    assert [{:field, _, _}, _] = query.fields

    query = Schema |> select([r], [r, r.x]) |> normalize
    assert_fields query, projection: %{_id: true, x: true, y: true, z: true, w: true}
    # This seems off -- why is x apearing 2 times?
    assert [{:field, :id, _}, {:field, :x, _}, {:field, :y, _}, {:field, :z, _}, {:field, :w, _}, {:field, :x, _}] = query.fields

    query = Schema |> select([r], [r]) |> normalize
    assert_fields query, projection: %{_id: true, x: true, y: true, z: true, w: true}
    assert [{:field, :id, _}, {:field, :x, _}, {:field, :y, _}, {:field, :z, _}, {:field, :w, _}] = query.fields

    query = Schema |> select([r], {1}) |> normalize
    assert_fields query, projection: %{}, fields: []

    query = Schema |> select([r], [r.id]) |> normalize
    assert_fields query, projection: %{_id: true}
    assert [{:field, :id, _}] = query.fields
  end

  test "count" do
    query = Schema |> select([r], count(r.id)) |> normalize
    assert %Mongo.Ecto.NormalizedQuery.CountQuery{} = query
    assert_fields query, coll: "schema", query: %{}

    query = Schema |> select([r], count(r.id)) |> where([r], r.x > 10) |> normalize
    assert %Mongo.Ecto.NormalizedQuery.CountQuery{} = query
    assert_fields query, coll: "schema", query: %{x: ["$gt": 10]}

    assert_raise Ecto.QueryError, fn ->
      Schema |> select([r], {r.id, count(r.id)}) |> normalize
    end

    query = Schema |> select([r], count(r.x, :distinct)) |> normalize
    assert %Mongo.Ecto.NormalizedQuery.AggregateQuery{} = query

    assert_fields query,
      pipeline: [["$group": [_id: "$x"]], ["$group": [_id: nil, value: ["$sum": 1]]]]
  end

  test "max" do
    group_stage = ["$group": [_id: nil, value: [{"$max", "$x"}]]]
    query = Schema |> select([r], max(r.x)) |> normalize
    assert_fields query, coll: "schema", pipeline: [group_stage]

    query = Schema |> select([r], max(r.x)) |> where([r], r.x == 10) |> normalize

    assert_fields query,
      coll: "schema",
      pipeline: [["$match": %{x: 10}], group_stage]

    query = Schema |> select([r], max(r.x)) |> limit([r], 3) |> offset([r], 5) |> normalize

    assert_fields query,
      coll: "schema",
      pipeline: [["$limit": 3], ["$skip": 5], group_stage]

    assert_raise Ecto.QueryError, fn ->
      Schema |> select([r], {max(r.x), r.id}) |> normalize
    end
  end

  test "min" do
    group_stage = ["$group": [_id: nil, value: [{"$min", "$x"}]]]
    query = Schema |> select([r], min(r.x)) |> normalize
    assert_fields query, coll: "schema", pipeline: [group_stage]

    query = Schema |> select([r], min(r.x)) |> where([r], r.x == 10) |> normalize

    assert_fields query,
      coll: "schema",
      pipeline: [["$match": %{x: 10}], group_stage]

    query = Schema |> select([r], min(r.x)) |> limit([r], 3) |> offset([r], 5) |> normalize

    assert_fields query,
      coll: "schema",
      pipeline: [["$limit": 3], ["$skip": 5], group_stage]

    assert_raise Ecto.QueryError, fn ->
      Schema |> select([r], {min(r.x), r.id}) |> normalize
    end
  end

  test "sum" do
    group_stage = ["$group": [_id: nil, value: [{"$sum", "$x"}]]]
    query = Schema |> select([r], sum(r.x)) |> normalize
    assert_fields query, coll: "schema", pipeline: [group_stage]

    query = Schema |> select([r], sum(r.x)) |> where([r], r.x == 10) |> normalize

    assert_fields query,
      coll: "schema",
      pipeline: [["$match": %{x: 10}], group_stage]

    query = Schema |> select([r], sum(r.x)) |> limit([r], 3) |> offset([r], 5) |> normalize

    assert_fields query,
      coll: "schema",
      pipeline: [["$limit": 3], ["$skip": 5], group_stage]

    assert_raise Ecto.QueryError, fn ->
      Schema |> select([r], {sum(r.x), r.id}) |> normalize
    end
  end

  test "avg" do
    group_stage = ["$group": [_id: nil, value: [{"$avg", "$x"}]]]
    query = Schema |> select([r], avg(r.x)) |> normalize
    assert_fields query, coll: "schema", pipeline: [group_stage]

    query = Schema |> select([r], avg(r.x)) |> where([r], r.x == 10) |> normalize

    assert_fields query,
      coll: "schema",
      pipeline: [["$match": %{x: 10}], group_stage]

    query = Schema |> select([r], avg(r.x)) |> limit([r], 3) |> offset([r], 5) |> normalize

    assert_fields query,
      coll: "schema",
      pipeline: [["$limit": 3], ["$skip": 5], group_stage]

    assert_raise Ecto.QueryError, fn ->
      Schema |> select([r], {avg(r.x), r.id}) |> normalize
    end
  end

  test "distinct" do
    assert_raise Ecto.QueryError, fn ->
      Schema |> distinct([r], r.x) |> select([r], {r.x, r.y}) |> normalize
    end

    assert_raise Ecto.QueryError, fn ->
      Schema |> distinct(true) |> select([r], {r.x, r.y}) |> normalize
    end
  end

  test "where" do
    query =
      Schema
      |> where([r], r.x == 42)
      |> where([r], r.y != 43)
      |> select([r], r.x)
      |> normalize

    assert_fields query, query: %{y: ["$ne": 43], x: 42}, projection: %{x: true}

    query = Schema |> where([r], r.x > 5) |> where([r], r.x < 10) |> normalize
    assert_fields query, query: %{x: ["$gt": 5, "$lt": 10]}

    query = Schema |> where([r], not (r.x == 42)) |> normalize
    assert_fields query, query: %{x: ["$ne": 42]}
  end

  test "order by" do
    query = Schema |> order_by([r], r.x) |> select([r], r.x) |> normalize
    assert_fields query, query: %{}, order: [x: 1]

    query = Schema |> order_by([r], [r.x, r.y]) |> select([r], r.x) |> normalize
    assert_fields query, query: %{}, order: [x: 1, y: 1]

    query = Schema |> order_by([r], asc: r.x, desc: r.y) |> select([r], r.x) |> normalize
    assert_fields query, query: %{}, order: [x: 1, y: -1]

    query = Schema |> order_by([r], []) |> select([r], r.x) |> normalize
    assert_fields query, query: %{}, order: %{}
  end

  test "limit and offset" do
    query = Schema |> limit([r], 3) |> normalize
    assert_fields query, opts: [limit: 3]

    query = Schema |> offset([r], 5) |> normalize
    assert_fields query, opts: [skip: 5]

    query = Schema |> offset([r], 5) |> limit([r], 3) |> normalize
    assert_fields query, opts: [limit: 3, skip: 5]
  end

  test "lock" do
    assert_raise Ecto.QueryError, fn ->
      Schema |> lock("FOR SHARE NOWAIT") |> normalize
    end
  end

  test "binary ops" do
    query = Schema |> where([r], r.x == 2) |> normalize
    assert_fields query, query: %{x: 2}

    query = Schema |> where([r], r.x != 2) |> normalize
    assert_fields query, query: %{x: ["$ne": 2]}

    query = Schema |> where([r], r.x <= 2) |> normalize
    assert_fields query, query: %{x: ["$lte": 2]}

    query = Schema |> where([r], r.x >= 2) |> normalize
    assert_fields query, query: %{x: ["$gte": 2]}

    query = Schema |> where([r], r.x < 2) |> normalize
    assert_fields query, query: %{x: ["$lt": 2]}

    query = Schema |> where([r], r.x > 2) |> normalize
    assert_fields query, query: %{x: ["$gt": 2]}
  end

  test "is_nil" do
    query = Schema |> where([r], is_nil(r.x)) |> normalize
    assert_fields query, query: %{x: nil}

    query = Schema |> where([r], not is_nil(r.x)) |> normalize
    assert_fields query, query: %{x: ["$ne": nil]}
  end

  test "sql fragments" do
    assert_raise Ecto.QueryError, fn ->
      Schema |> select([r], fragment("downcase(?)", r.x)) |> normalize
    end
  end

  test "fragments in where" do
    query = Schema |> where([], fragment(x: 1)) |> normalize
    assert_fields query, query: %{x: 1}

    query = Schema |> where([], fragment(x: ["$in": ^[1, 2, 3]])) |> normalize
    assert_fields query, query: %{x: ["$in": [1, 2, 3]]}

    query = Schema |> where([], fragment(^[x: 1])) |> normalize
    assert_fields query, query: %{x: 1}
  end

  test "fragments in select" do
    query = Schema |> select([], fragment("z.$": 1)) |> normalize
    assert_fields query, projection: %{"z.$": 1}
    assert [{:fragment, _, _}] = query.fields

    query = Schema |> select([r], {r.x, fragment("z.$": 1)}) |> normalize
    assert_fields query, projection: %{"z.$": 1, x: true}
    assert [{:field, :x, _}, {:fragment, _, _}] = query.fields
  end

  test "literals" do
    query = "schema" |> where(foo: true) |> select([], true) |> normalize
    assert_fields query, query: %{foo: true}

    query = "schema" |> where(foo: false) |> select([], true) |> normalize
    assert_fields query, query: %{foo: false}

    query = "schema" |> where(foo: "abc") |> select([], true) |> normalize
    assert_fields query, query: %{foo: "abc"}

    query = "schema" |> where(foo: <<0, ?a, ?b, ?c>>) |> select([], true) |> normalize
    assert_fields query, query: %{foo: %BSON.Binary{binary: <<0, ?a, ?b, ?c>>}}

    query = "schema" |> where(foo: 123) |> select([], true) |> normalize
    assert_fields query, query: %{foo: 123}

    query = "schema" |> where(foo: 123.0) |> select([], true) |> normalize
    assert_fields query, query: %{foo: 123.0}
  end

  # test "tagged type" do
  #   # query = Schema |> select([], type(^"601d74e4-a8d3-4b6e-8365-eddb4c893327", Ecto.UUID)) |> normalize

  #   # query = Schema |> select([], type(^1, Custom.Permalink)) |> normalize

  #   # query = Schema |> select([], type(^[1,2,3], {:array, Custom.Permalink})) |> normalize
  # end

  test "nested expressions" do
    z = 123

    query =
      from(r in Schema, [])
      |> where([r], (r.x > 0 and r.y > ^(-z)) or true)
      |> normalize

    assert_fields query, query: %{"$or": [["$and": [[x: ["$gt": 0]], [y: ["$gt": -123]]]], true]}
  end

  test "bool ops" do
    query = Schema |> where([], true and false) |> normalize
    assert_fields query, query: %{"$and": [true, false]}

    query = Schema |> where([], true or false) |> normalize
    assert_fields query, query: %{"$or": [true, false]}

    query = Schema |> where([r], not (r.x > 0) and not (r.x < 5)) |> normalize
    assert_fields query, query: %{"$and": [["$not": [x: ["$gt": 0]]], ["$not": [x: ["$lt": 5]]]]}
  end

  # test "in expression" do
  #   query = Schema |> select([e], 1 in []) |> normalize
  #   assert SQL.all(query) == ~s{SELECT false FROM "schema" AS m0}

  #   query = Schema |> select([e], 1 in [1,e.x,3]) |> normalize
  #   assert SQL.all(query) == ~s{SELECT 1 IN (1,m0."x",3) FROM "schema" AS m0}

  #   query = Schema |> select([e], 1 in ^[]) |> normalize
  #   assert SQL.all(query) == ~s{SELECT 1 = ANY($1) FROM "schema" AS m0}

  #   query = Schema |> select([e], 1 in ^[1, 2, 3]) |> normalize
  #   assert SQL.all(query) == ~s{SELECT 1 = ANY($1) FROM "schema" AS m0}

  #   query = Schema |> select([e], 1 in [1, ^2, 3]) |> normalize
  #   assert SQL.all(query) == ~s{SELECT 1 IN (1,$1,3) FROM "schema" AS m0}

  #   query = Schema |> select([e], ^1 in [1, ^2, 3]) |> normalize
  #   assert SQL.all(query) == ~s{SELECT $1 IN (1,$2,3) FROM "schema" AS m0}

  #   query = Schema |> select([e], ^1 in ^[1, 2, 3]) |> normalize
  #   assert SQL.all(query) == ~s{SELECT $1 = ANY($2) FROM "schema" AS m0}

  #   query = Schema |> select([e], 1 in e.w) |> normalize
  #   assert SQL.all(query) == ~s{SELECT 1 = ANY(m0."w") FROM "schema" AS m0}

  #   query = Schema |> select([e], 1 in fragment("foo")) |> normalize
  #   assert SQL.all(query) == ~s{SELECT 1 = ANY(foo) FROM "schema" AS m0}
  # end

  test "having" do
    assert_raise Ecto.QueryError, fn ->
      Schema |> having([p], p.x == p.x) |> normalize
    end
  end

  test "group by" do
    assert_raise Ecto.QueryError, fn ->
      Schema |> group_by([r], r.x) |> select([r], r.x) |> normalize
    end
  end

  # test "arrays and sigils" do
  #   query = Schema |> select([], fragment("?", [1, 2, 3])) |> normalize
  #   assert SQL.all(query) == ~s{SELECT ARRAY[1,2,3] FROM "schema" AS m0}

  #   query = Schema |> select([], fragment("?", ~w(abc def))) |> normalize
  #   assert SQL.all(query) == ~s{SELECT ARRAY['abc','def'] FROM "schema" AS m0}
  # end

  test "interpolated values" do
    query =
      Schema
      |> where([], ^true)
      |> where([], ^false)
      |> order_by([], ^:x)
      |> limit([], ^4)
      |> offset([], ^5)
      |> normalize

    assert_fields query,
      opts: [limit: 4, skip: 5],
      order: [x: 1],
      query: %{_id: ["$exists": false]}
  end

  # test "fragments and types" do
  #   query =
  #     normalize from(e in "schema",
  #       where: fragment("extract(? from ?) = ?", ^"month", e.start_time, type(^"4", :integer)),
  #       where: fragment("extract(? from ?) = ?", ^"year", e.start_time, type(^"2015", :integer)),
  #       select: true)

  #   result =
  #     "SELECT TRUE FROM \"schema\" AS m0 " <>
  #     "WHERE (extract($1 from m0.\"start_time\") = $2::integer) " <>
  #     "AND (extract($3 from m0.\"start_time\") = $4::integer)"

  #   assert SQL.all(query) == String.rstrip(result)
  # end

  # test "fragments allow ? to be escaped with backslash" do
  #   query =
  #     normalize  from(e in "schema",
  #       where: fragment("? = \"query\\?\"", e.start_time),
  #       select: true)

  #   result =
  #     "SELECT TRUE FROM \"schema\" AS m0 " <>
  #     "WHERE (m0.\"start_time\" = \"query?\")"

  #   assert SQL.all(query) == String.rstrip(result)
  # end

  # ## *_all

  # test "update all" do
  #   query = from(m in Schema, update: [set: [x: 0]]) |> normalize(:update_all)
  #   assert SQL.update_all(query) ==
  #          ~s{UPDATE "schema" AS m0 SET "x" = 0}

  #   query = from(m in Schema, update: [set: [x: 0], inc: [y: 1, z: -3]]) |> normalize(:update_all)
  #   assert SQL.update_all(query) ==
  #          ~s{UPDATE "schema" AS m0 SET "x" = 0, "y" = "y" + 1, "z" = "z" + -3}

  #   query = from(e in Schema, where: e.x == 123, update: [set: [x: 0]]) |> normalize(:update_all)
  #   assert SQL.update_all(query) ==
  #          ~s{UPDATE "schema" AS m0 SET "x" = 0 WHERE (m0."x" = 123)}

  #   query = from(m in Schema, update: [set: [x: ^0]]) |> normalize(:update_all)
  #   assert SQL.update_all(query) ==
  #          ~s{UPDATE "schema" AS m0 SET "x" = $1}

  #   query = Schema |> join(:inner, [p], q in Schema2, p.x == q.z)
  #                 |> update([_], set: [x: 0]) |> normalize(:update_all)
  #   assert SQL.update_all(query) ==
  #          ~s{UPDATE "schema" AS m0 SET "x" = 0 FROM "schema2" AS m1 WHERE (m0."x" = m1."z")}

  #   query = from(e in Schema, where: e.x == 123, update: [set: [x: 0]],
  #                            join: q in Schema2, on: e.x == q.z) |> normalize(:update_all)
  #   assert SQL.update_all(query) ==
  #          ~s{UPDATE "schema" AS m0 SET "x" = 0 FROM "schema2" AS m1 } <>
  #          ~s{WHERE (m0."x" = m1."z") AND (m0."x" = 123)}
  # end

  # test "update all with returning" do
  #   query = from(m in Schema, update: [set: [x: 0]]) |> select([m], m) |> normalize(:update_all)
  #   assert SQL.update_all(query) ==
  #          ~s{UPDATE "schema" AS m0 SET "x" = 0 RETURNING m0."id", m0."x", m0."y", m0."z", m0."w"}
  # end

  # test "update all array ops" do
  #   query = from(m in Schema, update: [push: [w: 0]]) |> normalize(:update_all)
  #   assert SQL.update_all(query) ==
  #          ~s{UPDATE "schema" AS m0 SET "w" = array_append("w", 0)}

  #   query = from(m in Schema, update: [pull: [w: 0]]) |> normalize(:update_all)
  #   assert SQL.update_all(query) ==
  #          ~s{UPDATE "schema" AS m0 SET "w" = array_remove("w", 0)}
  # end

  # test "update all with prefix" do
  #   query = from(m in Schema, update: [set: [x: 0]]) |> normalize(:update_all)
  #   assert SQL.update_all(%{query | prefix: "prefix"}) ==
  #          ~s{UPDATE "prefix"."schema" AS m0 SET "x" = 0}
  # end

  # test "delete all" do
  #   query = Schema |> Queryable.to_query |> normalize
  #   assert SQL.delete_all(query) == ~s{DELETE FROM "schema" AS m0}

  #   query = from(e in Schema, where: e.x == 123) |> normalize
  #   assert SQL.delete_all(query) ==
  #          ~s{DELETE FROM "schema" AS m0 WHERE (m0."x" = 123)}

  #   query = Schema |> join(:inner, [p], q in Schema2, p.x == q.z) |> normalize
  #   assert SQL.delete_all(query) ==
  #          ~s{DELETE FROM "schema" AS m0 USING "schema2" AS m1 WHERE m0."x" = m1."z"}

  #   query = from(e in Schema, where: e.x == 123, join: q in Schema2, on: e.x == q.z) |> normalize
  #   assert SQL.delete_all(query) ==
  #          ~s{DELETE FROM "schema" AS m0 USING "schema2" AS m1 WHERE m0."x" = m1."z" AND (m0."x" = 123)}
  # end

  # test "delete all with returning" do
  #   query = Schema |> Queryable.to_query |> select([m], m) |> normalize
  #   assert SQL.delete_all(query) == ~s{DELETE FROM "schema" AS m0 RETURNING m0."id", m0."x", m0."y", m0."z", m0."w"}
  # end

  # test "delete all with prefix" do
  #   query = Schema |> Queryable.to_query |> normalize
  #   assert SQL.delete_all(%{query | prefix: "prefix"}) == ~s{DELETE FROM "prefix"."schema" AS m0}
  # end

  # ## Joins

  # test "join" do
  #   query = Schema |> join(:inner, [p], q in Schema2, p.x == q.z) |> select([], true) |> normalize
  #   assert SQL.all(query) ==
  #          ~s{SELECT TRUE FROM "schema" AS m0 INNER JOIN "schema2" AS m1 ON m0."x" = m1."z"}

  #   query = Schema |> join(:inner, [p], q in Schema2, p.x == q.z)
  #                 |> join(:inner, [], Schema, true) |> select([], true) |> normalize
  #   assert SQL.all(query) ==
  #          ~s{SELECT TRUE FROM "schema" AS m0 INNER JOIN "schema2" AS m1 ON m0."x" = m1."z" } <>
  #          ~s{INNER JOIN "schema" AS m2 ON TRUE}
  # end

  # test "join with nothing bound" do
  #   query = Schema |> join(:inner, [], q in Schema2, q.z == q.z) |> select([], true) |> normalize
  #   assert SQL.all(query) ==
  #          ~s{SELECT TRUE FROM "schema" AS m0 INNER JOIN "schema2" AS m1 ON m1."z" = m1."z"}
  # end

  # test "join without schema" do
  #   query = "posts" |> join(:inner, [p], q in "comments", p.x == q.z) |> select([], true) |> normalize
  #   assert SQL.all(query) ==
  #          ~s{SELECT TRUE FROM "posts" AS p0 INNER JOIN "comments" AS c1 ON p0."x" = c1."z"}
  # end

  # test "join with subquery" do
  #   posts = subquery("posts" |> where(title: ^"hello") |> select([r], {r.x, r.y}))

  #   query = "comments" |> join(:inner, [c], p in subquery(posts), true) |> select([_, p], p.x) |> normalize
  #   assert SQL.all(query) ==
  #          ~s{SELECT s1."x" FROM "comments" AS c0 } <>
  #          ~s{INNER JOIN (SELECT p0."x", p0."y" FROM "posts" AS p0 WHERE (p0."title" = $1)) AS s1 ON TRUE}

  #   query = "comments" |> join(:inner, [c], p in subquery(posts), true) |> select([_, p], p) |> normalize
  #   assert SQL.all(query) ==
  #          ~s{SELECT s1."x", s1."y" FROM "comments" AS c0 } <>
  #          ~s{INNER JOIN (SELECT p0."x", p0."y" FROM "posts" AS p0 WHERE (p0."title" = $1)) AS s1 ON TRUE}
  # end

  # test "join with prefix" do
  #   query = Schema |> join(:inner, [p], q in Schema2, p.x == q.z) |> select([], true) |> normalize
  #   assert SQL.all(%{query | prefix: "prefix"}) ==
  #          ~s{SELECT TRUE FROM "prefix"."schema" AS m0 INNER JOIN "prefix"."schema2" AS m1 ON m0."x" = m1."z"}
  # end

  # test "join with fragment" do
  #   query = Schema
  #           |> join(:inner, [p], q in fragment("SELECT * FROM schema2 AS m2 WHERE m2.id = ? AND m2.field = ?", p.x, ^10))
  #           |> select([p], {p.id, ^0})
  #           |> where([p], p.id > 0 and p.id < ^100)
  #           |> normalize
  #   assert SQL.all(query) ==
  #          ~s{SELECT m0."id", $1 FROM "schema" AS m0 INNER JOIN } <>
  #          ~s{(SELECT * FROM schema2 AS m2 WHERE m2.id = m0."x" AND m2.field = $2) AS f1 ON TRUE } <>
  #          ~s{WHERE ((m0."id" > 0) AND (m0."id" < $3))}
  # end

  # test "join with fragment and on defined" do
  #   query = Schema
  #           |> join(:inner, [p], q in fragment("SELECT * FROM schema2"), q.id == p.id)
  #           |> select([p], {p.id, ^0})
  #           |> normalize
  #   assert SQL.all(query) ==
  #          ~s{SELECT m0."id", $1 FROM "schema" AS m0 INNER JOIN } <>
  #          ~s{(SELECT * FROM schema2) AS f1 ON f1."id" = m0."id"}
  # end

  # test "lateral join with fragment" do
  #   query = Schema
  #           |> join(:inner_lateral, [p], q in fragment("SELECT * FROM schema2 AS m2 WHERE m2.id = ? AND m2.field = ?", p.x, ^10))
  #           |> select([p, q], {p.id, q.z})
  #           |> where([p], p.id > 0 and p.id < ^100)
  #           |> normalize
  #   assert SQL.all(query) ==
  #          ~s{SELECT m0."id", f1."z" FROM "schema" AS m0 INNER JOIN LATERAL } <>
  #          ~s{(SELECT * FROM schema2 AS m2 WHERE m2.id = m0."x" AND m2.field = $1) AS f1 ON TRUE } <>
  #          ~s{WHERE ((m0."id" > 0) AND (m0."id" < $2))}
  # end

  # ## Associations

  # test "association join belongs_to" do
  #   query = Schema2 |> join(:inner, [c], p in assoc(c, :post)) |> select([], true) |> normalize
  #   assert SQL.all(query) ==
  #          "SELECT TRUE FROM \"schema2\" AS m0 INNER JOIN \"schema\" AS m1 ON m1.\"x\" = m0.\"z\""
  # end

  # test "association join has_many" do
  #   query = Schema |> join(:inner, [p], c in assoc(p, :comments)) |> select([], true) |> normalize
  #   assert SQL.all(query) ==
  #          "SELECT TRUE FROM \"schema\" AS m0 INNER JOIN \"schema2\" AS m1 ON m1.\"z\" = m0.\"x\""
  # end

  # test "association join has_one" do
  #   query = Schema |> join(:inner, [p], pp in assoc(p, :permalink)) |> select([], true) |> normalize
  #   assert SQL.all(query) ==
  #          "SELECT TRUE FROM \"schema\" AS m0 INNER JOIN \"schema3\" AS m1 ON m1.\"id\" = m0.\"y\""
  # end

  # test "join produces correct bindings" do
  #   query = from(p in Schema, join: c in Schema2, on: true)
  #   query = from(p in query, join: c in Schema2, on: true, select: {p.id, c.id})
  #   query = normalize(query)
  #   assert SQL.all(query) ==
  #          "SELECT m0.\"id\", m2.\"id\" FROM \"schema\" AS m0 INNER JOIN \"schema2\" AS m1 ON TRUE INNER JOIN \"schema2\" AS m2 ON TRUE"
  # end

  # # Schema based

  # test "insert" do
  #   query = SQL.insert(nil, "schema", [:x, :y], [[:x, :y]], [:id])
  #   assert query == ~s{INSERT INTO "schema" ("x","y") VALUES ($1,$2) RETURNING "id"}

  #   query = SQL.insert(nil, "schema", [:x, :y], [[:x, :y], [nil, :z]], [:id])
  #   assert query == ~s{INSERT INTO "schema" ("x","y") VALUES ($1,$2),(DEFAULT,$3) RETURNING "id"}

  #   query = SQL.insert(nil, "schema", [], [[]], [:id])
  #   assert query == ~s{INSERT INTO "schema" VALUES (DEFAULT) RETURNING "id"}

  #   query = SQL.insert(nil, "schema", [], [[]], [])
  #   assert query == ~s{INSERT INTO "schema" VALUES (DEFAULT)}

  #   query = SQL.insert("prefix", "schema", [], [[]], [])
  #   assert query == ~s{INSERT INTO "prefix"."schema" VALUES (DEFAULT)}
  # end

  # test "update" do
  #   query = SQL.update(nil, "schema", [:x, :y], [:id], [])
  #   assert query == ~s{UPDATE "schema" SET "x" = $1, "y" = $2 WHERE "id" = $3}

  #   query = SQL.update(nil, "schema", [:x, :y], [:id], [:z])
  #   assert query == ~s{UPDATE "schema" SET "x" = $1, "y" = $2 WHERE "id" = $3 RETURNING "z"}

  #   query = SQL.update("prefix", "schema", [:x, :y], [:id], [])
  #   assert query == ~s{UPDATE "prefix"."schema" SET "x" = $1, "y" = $2 WHERE "id" = $3}
  # end

  # test "delete" do
  #   query = SQL.delete(nil, "schema", [:x, :y], [])
  #   assert query == ~s{DELETE FROM "schema" WHERE "x" = $1 AND "y" = $2}

  #   query = SQL.delete(nil, "schema", [:x, :y], [:z])
  #   assert query == ~s{DELETE FROM "schema" WHERE "x" = $1 AND "y" = $2 RETURNING "z"}

  #   query = SQL.delete("prefix", "schema", [:x, :y], [])
  #   assert query == ~s{DELETE FROM "prefix"."schema" WHERE "x" = $1 AND "y" = $2}
  # end

  # # DDL

  # import Ecto.Migration, only: [table: 1, table: 2, index: 2, index: 3, references: 1,
  #                               references: 2, constraint: 2, constraint: 3]

  # test "executing a string during migration" do
  #   assert SQL.execute_ddl("example") == "example"
  # end

  # test "create table" do
  #   create = {:create, table(:posts),
  #              [{:add, :name, :string, [default: "Untitled", size: 20, null: false]},
  #               {:add, :price, :numeric, [precision: 8, scale: 2, default: {:fragment, "expr"}]},
  #               {:add, :on_hand, :integer, [default: 0, null: true]},
  #               {:add, :is_active, :boolean, [default: true]},
  #               {:add, :tags, {:array, :string}, [default: []]}]}

  #   assert SQL.execute_ddl(create) == """
  #   CREATE TABLE "posts" ("name" varchar(20) DEFAULT 'Untitled' NOT NULL,
  #   "price" numeric(8,2) DEFAULT expr,
  #   "on_hand" integer DEFAULT 0 NULL,
  #   "is_active" boolean DEFAULT true,
  #   "tags" varchar(255)[] DEFAULT ARRAY[]::varchar[])
  #   """ |> remove_newlines
  # end

  # test "create table with prefix" do
  #   create = {:create, table(:posts, prefix: :foo),
  #              [{:add, :category_0, references(:categories), []}]}

  #   assert SQL.execute_ddl(create) == """
  #   CREATE TABLE "foo"."posts"
  #   ("category_0" integer CONSTRAINT "posts_category_0_fkey" REFERENCES "foo"."categories"("id"))
  #   """ |> remove_newlines
  # end

  # test "create table with references" do
  #   create = {:create, table(:posts),
  #              [{:add, :id, :serial, [primary_key: true]},
  #               {:add, :category_0, references(:categories), []},
  #               {:add, :category_1, references(:categories, name: :foo_bar), []},
  #               {:add, :category_2, references(:categories, on_delete: :nothing), []},
  #               {:add, :category_3, references(:categories, on_delete: :delete_all), [null: false]},
  #               {:add, :category_4, references(:categories, on_delete: :nilify_all), []},
  #               {:add, :category_5, references(:categories, on_update: :nothing), []},
  #               {:add, :category_6, references(:categories, on_update: :update_all), [null: false]},
  #               {:add, :category_7, references(:categories, on_update: :nilify_all), []},
  #               {:add, :category_8, references(:categories, on_delete: :nilify_all, on_update: :update_all), [null: false]}]}

  #   assert SQL.execute_ddl(create) == """
  #   CREATE TABLE "posts" ("id" serial,
  #   "category_0" integer CONSTRAINT "posts_category_0_fkey" REFERENCES "categories"("id"),
  #   "category_1" integer CONSTRAINT "foo_bar" REFERENCES "categories"("id"),
  #   "category_2" integer CONSTRAINT "posts_category_2_fkey" REFERENCES "categories"("id"),
  #   "category_3" integer NOT NULL CONSTRAINT "posts_category_3_fkey" REFERENCES "categories"("id") ON DELETE CASCADE,
  #   "category_4" integer CONSTRAINT "posts_category_4_fkey" REFERENCES "categories"("id") ON DELETE SET NULL,
  #   "category_5" integer CONSTRAINT "posts_category_5_fkey" REFERENCES "categories"("id"),
  #   "category_6" integer NOT NULL CONSTRAINT "posts_category_6_fkey" REFERENCES "categories"("id") ON UPDATE CASCADE,
  #   "category_7" integer CONSTRAINT "posts_category_7_fkey" REFERENCES "categories"("id") ON UPDATE SET NULL,
  #   "category_8" integer NOT NULL CONSTRAINT "posts_category_8_fkey" REFERENCES "categories"("id") ON DELETE SET NULL ON UPDATE CASCADE,
  #   PRIMARY KEY ("id"))
  #   """ |> remove_newlines
  # end

  # test "create table with options" do
  #   create = {:create, table(:posts, [options: "WITH FOO=BAR"]),
  #              [{:add, :id, :serial, [primary_key: true]},
  #               {:add, :created_at, :datetime, []}]}
  #   assert SQL.execute_ddl(create) ==
  #          ~s|CREATE TABLE "posts" ("id" serial, "created_at" timestamp, PRIMARY KEY ("id")) WITH FOO=BAR|
  # end

  # test "create table with composite key" do
  #   create = {:create, table(:posts),
  #              [{:add, :a, :integer, [primary_key: true]},
  #               {:add, :b, :integer, [primary_key: true]},
  #               {:add, :name, :string, []}]}

  #   assert SQL.execute_ddl(create) == """
  #   CREATE TABLE "posts" ("a" integer, "b" integer, "name" varchar(255), PRIMARY KEY ("a", "b"))
  #   """ |> remove_newlines
  # end

  # test "drop table" do
  #   drop = {:drop, table(:posts)}
  #   assert SQL.execute_ddl(drop) == ~s|DROP TABLE "posts"|
  # end

  # test "drop table with prefix" do
  #   drop = {:drop, table(:posts, prefix: :foo)}
  #   assert SQL.execute_ddl(drop) == ~s|DROP TABLE "foo"."posts"|
  # end

  # test "alter table" do
  #   alter = {:alter, table(:posts),
  #              [{:add, :title, :string, [default: "Untitled", size: 100, null: false]},
  #               {:add, :author_id, references(:author), []},
  #               {:modify, :price, :numeric, [precision: 8, scale: 2, null: true]},
  #               {:modify, :cost, :integer, [null: false, default: nil]},
  #               {:modify, :permalink_id, references(:permalinks), null: false},
  #               {:remove, :summary}]}

  #   assert SQL.execute_ddl(alter) == """
  #   ALTER TABLE "posts"
  #   ADD COLUMN "title" varchar(100) DEFAULT 'Untitled' NOT NULL,
  #   ADD COLUMN "author_id" integer CONSTRAINT "posts_author_id_fkey" REFERENCES "author"("id"),
  #   ALTER COLUMN "price" TYPE numeric(8,2) ,
  #   ALTER COLUMN "price" DROP NOT NULL,
  #   ALTER COLUMN "cost" TYPE integer ,
  #   ALTER COLUMN "cost" SET NOT NULL ,
  #   ALTER COLUMN "cost" SET DEFAULT NULL,
  #   ALTER COLUMN "permalink_id" TYPE integer ,
  #   ADD CONSTRAINT "posts_permalink_id_fkey" FOREIGN KEY ("permalink_id") REFERENCES "permalinks"("id") ,
  #   ALTER COLUMN "permalink_id" SET NOT NULL,
  #   DROP COLUMN "summary"
  #   """ |> remove_newlines
  # end

  # test "alter table with prefix" do
  #   alter = {:alter, table(:posts, prefix: :foo),
  #              [{:add, :author_id, references(:author, prefix: :foo), []},
  #               {:modify, :permalink_id, references(:permalinks, prefix: :foo), null: false}]}

  #   assert SQL.execute_ddl(alter) == """
  #   ALTER TABLE "foo"."posts"
  #   ADD COLUMN "author_id" integer CONSTRAINT "posts_author_id_fkey" REFERENCES "foo"."author"("id"),
  #   ALTER COLUMN \"permalink_id\" TYPE integer ,
  #   ADD CONSTRAINT "posts_permalink_id_fkey" FOREIGN KEY ("permalink_id") REFERENCES "foo"."permalinks"("id") ,
  #   ALTER COLUMN "permalink_id" SET NOT NULL
  #   """ |> remove_newlines
  # end

  # test "create index" do
  #   create = {:create, index(:posts, [:category_id, :permalink])}
  #   assert SQL.execute_ddl(create) ==
  #          ~s|CREATE INDEX "posts_category_id_permalink_index" ON "posts" ("category_id", "permalink")|

  #   create = {:create, index(:posts, ["lower(permalink)"], name: "posts$main")}
  #   assert SQL.execute_ddl(create) ==
  #          ~s|CREATE INDEX "posts$main" ON "posts" (lower(permalink))|
  # end

  # test "create index with prefix" do
  #   create = {:create, index(:posts, [:category_id, :permalink], prefix: :foo)}
  #   assert SQL.execute_ddl(create) ==
  #          ~s|CREATE INDEX "posts_category_id_permalink_index" ON "foo"."posts" ("category_id", "permalink")|

  #   create = {:create, index(:posts, ["lower(permalink)"], name: "posts$main", prefix: :foo)}
  #   assert SQL.execute_ddl(create) ==
  #          ~s|CREATE INDEX "posts$main" ON "foo"."posts" (lower(permalink))|
  # end

  # test "create unique index" do
  #   create = {:create, index(:posts, [:permalink], unique: true)}
  #   assert SQL.execute_ddl(create) ==
  #          ~s|CREATE UNIQUE INDEX "posts_permalink_index" ON "posts" ("permalink")|
  # end

  # test "create unique index with condition" do
  #   create = {:create, index(:posts, [:permalink], unique: true, where: "public IS TRUE")}
  #   assert SQL.execute_ddl(create) ==
  #          ~s|CREATE UNIQUE INDEX "posts_permalink_index" ON "posts" ("permalink") WHERE public IS TRUE|

  #   create = {:create, index(:posts, [:permalink], unique: true, where: :public)}
  #   assert SQL.execute_ddl(create) ==
  #          ~s|CREATE UNIQUE INDEX "posts_permalink_index" ON "posts" ("permalink") WHERE public|
  # end

  # test "create index concurrently" do
  #   create = {:create, index(:posts, [:permalink], concurrently: true)}
  #   assert SQL.execute_ddl(create) ==
  #          ~s|CREATE INDEX CONCURRENTLY "posts_permalink_index" ON "posts" ("permalink")|
  # end

  # test "create unique index concurrently" do
  #   create = {:create, index(:posts, [:permalink], concurrently: true, unique: true)}
  #   assert SQL.execute_ddl(create) ==
  #          ~s|CREATE UNIQUE INDEX CONCURRENTLY "posts_permalink_index" ON "posts" ("permalink")|
  # end

  # test "create an index using a different type" do
  #   create = {:create, index(:posts, [:permalink], using: :hash)}
  #   assert SQL.execute_ddl(create) ==
  #          ~s|CREATE INDEX "posts_permalink_index" ON "posts" USING hash ("permalink")|
  # end

  # test "drop index" do
  #   drop = {:drop, index(:posts, [:id], name: "posts$main")}
  #   assert SQL.execute_ddl(drop) == ~s|DROP INDEX "posts$main"|
  # end

  # test "drop index with prefix" do
  #   drop = {:drop, index(:posts, [:id], name: "posts$main", prefix: :foo)}
  #   assert SQL.execute_ddl(drop) == ~s|DROP INDEX "foo"."posts$main"|
  # end

  # test "drop index concurrently" do
  #   drop = {:drop, index(:posts, [:id], name: "posts$main", concurrently: true)}
  #   assert SQL.execute_ddl(drop) == ~s|DROP INDEX CONCURRENTLY "posts$main"|
  # end

  # test "create check constraint" do
  #   create = {:create, constraint(:products, "price_must_be_positive", check: "price > 0")}
  #   assert SQL.execute_ddl(create) ==
  #          ~s|ALTER TABLE "products" ADD CONSTRAINT "price_must_be_positive" CHECK (price > 0)|

  #   create = {:create, constraint(:products, "price_must_be_positive", check: "price > 0", prefix: "foo")}
  #   assert SQL.execute_ddl(create) ==
  #          ~s|ALTER TABLE "foo"."products" ADD CONSTRAINT "price_must_be_positive" CHECK (price > 0)|
  # end

  # test "create exclusion constraint" do
  #   create = {:create, constraint(:products, "price_must_be_positive", exclude: ~s|gist (int4range("from", "to", '[]') WITH &&)|)}
  #   assert SQL.execute_ddl(create) ==
  #          ~s|ALTER TABLE "products" ADD CONSTRAINT "price_must_be_positive" EXCLUDE USING gist (int4range("from", "to", '[]') WITH &&)|
  # end

  # test "drop constraint" do
  #   drop = {:drop, constraint(:products, "price_must_be_positive")}
  #   assert SQL.execute_ddl(drop) ==
  #   ~s|ALTER TABLE "products" DROP CONSTRAINT "price_must_be_positive"|

  #   drop = {:drop, constraint(:products, "price_must_be_positive", prefix: "foo")}
  #   assert SQL.execute_ddl(drop) ==
  #   ~s|ALTER TABLE "foo"."products" DROP CONSTRAINT "price_must_be_positive"|
  # end

  # test "rename table" do
  #   rename = {:rename, table(:posts), table(:new_posts)}
  #   assert SQL.execute_ddl(rename) == ~s|ALTER TABLE "posts" RENAME TO "new_posts"|
  # end

  # test "rename table with prefix" do
  #   rename = {:rename, table(:posts, prefix: :foo), table(:new_posts, prefix: :foo)}
  #   assert SQL.execute_ddl(rename) == ~s|ALTER TABLE "foo"."posts" RENAME TO "foo"."new_posts"|
  # end

  # test "rename column" do
  #   rename = {:rename, table(:posts), :given_name, :first_name}
  #   assert SQL.execute_ddl(rename) == ~s|ALTER TABLE "posts" RENAME "given_name" TO "first_name"|
  # end

  # test "rename column in prefixed table" do
  #   rename = {:rename, table(:posts, prefix: :foo), :given_name, :first_name}
  #   assert SQL.execute_ddl(rename) == ~s|ALTER TABLE "foo"."posts" RENAME "given_name" TO "first_name"|
  # end

  # defp remove_newlines(string) do
  #   string |> String.strip |> String.replace("\n", " ")
  # end
end
