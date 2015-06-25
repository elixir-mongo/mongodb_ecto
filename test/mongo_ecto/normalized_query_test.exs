defmodule Mongo.Ecto.NormalizedQueryTest do
  use ExUnit.Case, async: true

  alias Mongo.Ecto.NormalizedQuery
  import Ecto.Query

  defmodule Model do
    use Ecto.Model

    schema "model" do
      field :x, :integer
      field :y, :integer
      field :z, {:array, :integer}
    end
  end

  defp all(query) do
    {query, params} = Ecto.Query.Planner.prepare(query, [], %{})
    query
    |> Ecto.Query.Planner.normalize([], [])
    |> NormalizedQuery.all(params)
  end

  defmacro assert_query(query, kw) do
    Enum.map(kw, fn {key, value} ->
      quote do
        assert unquote(query).unquote(key) == unquote(value)
      end
    end)
  end

  test "bare model" do
    query = Model |> from |> all
    assert_query(query, coll: "model", query: %{},
                 projection: %{_id: true, x: true, y: true, z: true},
                 fields: [model: {Model, "model"}], opts: [num_return: 0, num_skip: 0])
  end

  test "from without model" do
    query = "posts" |> select([r], r.x) |> all
    assert_query(query, coll: "posts", projection: %{x: true},
                                       fields: [field: :x])

    query = "posts" |> select([r], {r, r.x}) |> all
    assert_query(query, coll: "posts", projection: %{},
                                       fields: [document: nil, field: :x])
  end

  test "where" do
    query = Model |> where([r], r.x == 42) |> where([r], r.y != 43)
                  |> select([r], r.x) |> all
    assert_query(query, query: [x: 42, y: ["$ne": 43]], projection: %{x: true})

    query = Model |> where([r], not (r.x == 42)) |> all
    assert_query(query, query: [x: ["$ne": 42]])
  end

  test "select" do
    query = Model |> select([r], {r.x, r.y}) |> all
    assert_query(query, projection: %{y: true, x: true},
                        fields: [field: :x, field: :y])

    query = Model |> select([r], [r.x, r.y]) |> all
    assert_query(query, projection: %{y: true, x: true},
                        fields: [field: :x, field: :y])

    query = Model |> select([r], [r, r.x]) |> all
    assert_query(query, projection: %{_id: true, x: true, y: true, z: true},
                        fields: [model: {Model, "model"}, field: :x])

    query = Model |> select([r], [r]) |> all
    assert_query(query, projection: %{_id: true, x: true, y: true, z: true},
                        fields: [model: {Model, "model"}])

    query = Model |> select([r], {1}) |> all
    assert_query(query, projection: %{},
                        fields: [1])

    query = Model |> select([r], [r.id]) |> all
    assert_query(query, projection: %{_id: true},
                        fields: [field: :id])

    query = from(r in Model) |> all
    assert_query(query, projection: %{_id: true, x: true, y: true, z: true},
                        fields: [model: {Model, "model"}])
  end

  test "order by" do
    query = Model |> order_by([r], r.x) |> select([r], r.x) |> all
    assert_query(query, query: ["$query": %{}, "$orderby": [x: 1]])

    query = Model |> order_by([r], [r.x, r.y]) |> select([r], r.x) |> all
    assert_query(query, query: ["$query": %{}, "$orderby": [x: 1, y: 1]])

    query = Model |> order_by([r], [asc: r.x, desc: r.y]) |> select([r], r.x) |> all
    assert_query(query, query: ["$query": %{}, "$orderby": [x: 1, y: -1]])

    query = Model |> order_by([r], []) |> select([r], r.x) |> all
    assert_query(query, query: %{})
  end

  test "limit and offset" do
    query = Model |> limit([r], 3) |> all
    assert_query(query, opts: [num_return: 3, num_skip: 0])

    query = Model |> offset([r], 5) |> all
    assert_query(query, opts: [num_return: 0, num_skip: 5])

    query = Model |> offset([r], 5) |> limit([r], 3) |> all
    assert_query(query, opts: [num_return: 3, num_skip: 5])
  end

  test "lock" do
    assert_raise Ecto.QueryError, fn ->
      Model |> lock("FOR SHARE NOWAIT") |> all
    end
  end

  test "sql fragments" do
    assert_raise Ecto.QueryError, fn ->
      Model |> select([r], fragment("downcase(?)", r.x)) |> all
    end
  end

  test "fragments in where" do
    query = Model |> where([], fragment(x: 1)) |> all
    assert_query(query, query: [x: 1])

    query = Model |> where([], fragment(x: ["$in": ^[1, 2, 3]])) |> all
    assert_query(query, query: [x: ["$in": [1, 2, 3]]])

    query = Model |> where([], fragment(^[x: 1])) |> all
    assert_query(query, query: [x: 1])
  end

  test "fragments in select" do
    query = Model |> select([], fragment("z.$": 1)) |> all
    assert_query(query, projection: %{"z.$": 1},
                        fields: [document: nil])

    query = Model |> select([r], {r.x, fragment("z.$": 1)}) |> all
    assert_query(query, projection: %{"z.$": 1, x: true},
                        fields: [field: :x, document: nil])
  end

  test "distinct" do
    assert_raise Ecto.QueryError, fn ->
      Model |> distinct([r], r.x) |> select([r], {r.x, r.y}) |> all
    end

    assert_raise Ecto.QueryError, fn ->
      Model |> distinct(true) |> select([r], {r.x, r.y}) |> all
    end
  end

  test "is_nil" do
    query = Model |> where([r], is_nil(r.x)) |> all
    assert_query(query, query: [x: nil])

    query = Model |> where([r], not is_nil(r.x)) |> all
    assert_query(query, query: [x: ["$ne": nil]])
  end

  test "literals" do
    query = Model |> select([], nil) |> all
    assert_query(query, fields: [nil])

    query = "plain" |> select([r], r.x) |> where([r], r.x == true) |> all
    assert_query(query, query: [x: true])

    query = "plain" |> select([r], r.x) |> where([r], r.x == false) |> all
    assert_query(query, query: [x: false])

    query = "plain" |> select([r], r.x) |> where([r], r.x == "abc") |> all
    assert_query(query, query: [x: "abc"])

    query = "plain" |> select([r], r.x) |> where([r], r.x == 123) |> all
    assert_query(query, query: [x: 123])

    query = "plain" |> select([r], r.x) |> where([r], r.x == 123.0) |> all
    assert_query(query, query: [x: 123.0])
  end

  test "nested expressions" do
    z = 123
    query = from(r in Model, [])
                      |> where([r], r.x > 0 and (r.y > ^(-z)) or true) |> all
    assert_query(query, query:
                 ["$or": [["$and": [[x: ["$gt": 0]], [y: ["$gt": -123]]]], true]])
  end

  test "binary ops" do
    query = Model |> where([r], r.x == 2) |> all
    assert_query(query, query: [x: 2])

    query = Model |> where([r], r.x != 2) |> all
    assert_query(query, query: [x: ["$ne": 2]])

    query = Model |> where([r], r.x <= 2) |> all
    assert_query(query, query: [x: ["$lte": 2]])

    query = Model |> where([r], r.x >= 2) |> all
    assert_query(query, query: [x: ["$gte": 2]])

    query = Model |> where([r], r.x < 2) |> all
    assert_query(query, query: [x: ["$lt": 2]])

    query = Model |> where([r], r.x > 2) |> all
    assert_query(query, query: [x: ["$gt": 2]])
  end

  test "bool ops" do
    query = Model |> where([], true and false) |> all
    assert_query(query, query: ["$and": [true, false]])

    query = Model |> where([], true or false) |> all
    assert_query(query, query: ["$or": [true, false]])

    query = Model |> where([r], not (r.x > 0) and not (r.x < 5)) |> all
    assert_query(query, query:
                 ["$and": [["$not": [x: ["$gt": 0]]], ["$not": [x: ["$lt": 5]]]]])
  end

  test "in expression" do
    query = Model |> where([e], e.x in []) |> all
    assert_query(query, query: [x: ["$in": []]])

    query = Model |> where([e], e.x in ^[1, 2, 3]) |> all
    assert_query(query, query: [x: ["$in": [1, 2, 3]]])

    query = Model |> where([e], e.x in [1, ^2, 3]) |> all
    assert_query(query, query: [x: ["$in": [1, 2, 3]]])

    query = Model |> where([e], 1 in e.z) |> all
    assert_query(query, query: [z: 1])

    assert_raise Ecto.QueryError, fn ->
      Model |> where([e], 1 in ^[]) |> all
    end

    assert_raise Ecto.QueryError, fn ->
      Model |> where([e], e.x in [1, e.x, 3]) |> all
    end
  end

  test "having" do
    assert_raise Ecto.QueryError, fn ->
      Model |> having([p], p.x == p.x) |> all
    end
  end

  test "group by" do
    assert_raise Ecto.QueryError, fn ->
      Model |> group_by([r], r.x) |> select([r], r.x) |> all
    end
  end
end
