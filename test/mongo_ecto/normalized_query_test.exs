defmodule Mongo.Ecto.NormalizedQueryTest do
  use ExUnit.Case, async: true

  alias Mongo.Ecto.NormalizedQuery
  import Ecto.Query

  defmodule Model do
    use Ecto.Model

    schema "model" do
      field :x, :integer
      field :y, :integer
    end
  end

  defp normalize(query) do
    {query, params} = Ecto.Query.Planner.prepare(query, [], %{})
    query
    |> Ecto.Query.Planner.normalize([], [])
    |> NormalizedQuery.from_query(params)
  end

  defmacro assert_query(query, kw) do
    Enum.map(kw, fn {key, value} ->
      quote do
        assert unquote(query).unquote(key) == unquote(value)
      end
    end)
  end

  test "bare model" do
    query = Model |> from |> normalize
    assert_query(query, from: {"model", Model, :id}, query_order: %{},
                 projection: %{}, num_skip: 0, num_return: 0)
  end

  test "from without model" do
    query = "posts" |> select([r], r.x) |> normalize
    assert_query(query, from: {"posts", nil, nil}, projection: %{x: true})
  end

  test "where" do
    query = Model |> where([r], r.x == 42) |> where([r], r.y != 43)
                  |> select([r], r.x) |> normalize
    assert_query(query, query_order: %{x: 42, y: %{"$ne": 43}}, projection: %{x: true})

    query = Model |> where([r], not (r.x == 42)) |> normalize
    assert_query(query, query_order: %{x: %{"$neq": 42}})
  end

  test "select" do
    query = Model |> select([r], {r.x, r.y}) |> normalize
    assert_query(query, projection: %{x: true, y: true})

    query = Model |> select([r], [r.x, r.y]) |> normalize
    assert_query(query, projection: %{x: true, y: true})
  end

  test "order by" do
    query = Model |> order_by([r], r.x) |> select([r], r.x) |> normalize
    assert_query(query, query_order: %{"$query": %{}, "$orderby": %{x: 1}})

    query = Model |> order_by([r], [r.x, r.y]) |> select([r], r.x) |> normalize
    assert_query(query, query_order: %{"$query": %{}, "$orderby": %{x: 1, y: 1}})

    query = Model |> order_by([r], [asc: r.x, desc: r.y]) |> select([r], r.x) |> normalize
    assert_query(query, query_order: %{"$query": %{}, "$orderby": %{x: 1, y: -1}})

    query = Model |> order_by([r], []) |> select([r], r.x) |> normalize
    assert_query(query, query_order: %{})
  end

  test "limit and offset" do
    query = Model |> limit([r], 3) |> normalize
    assert_query(query, num_return: 3)

    query = Model |> offset([r], 5) |> normalize
    assert_query(query, num_skip: 5)

    query = Model |> offset([r], 5) |> limit([r], 3) |> normalize
    assert_query(query, num_return: 3, num_skip: 5)
  end

  test "lock" do
    assert_raise Ecto.QueryError, fn ->
      Model |> lock("FOR SHARE NOWAIT") |> normalize
    end
  end

  test "fragment" do
    assert_raise Ecto.QueryError, fn ->
      Model |> select([r], fragment("downcase(?)", r.x)) |> normalize
    end
  end

  test "distinct" do
    assert_raise Ecto.QueryError, fn ->
      Model |> distinct([r], r.x) |> select([r], {r.x, r.y}) |> normalize
    end

    assert_raise Ecto.QueryError, fn ->
      Model |> distinct(true) |> select([r], {r.x, r.y}) |> normalize
    end
  end

  test "is_nil" do
    query = Model |> where([r], is_nil(r.x)) |> normalize
    assert_query(query, query_order: %{x: nil})

    query = Model |> where([r], not is_nil(r.x)) |> normalize
    assert_query(query, query_order: %{x: %{"$neq": nil}})
  end

  test "literals" do
    query = Model |> select([], nil) |> normalize
    assert_query(query, fields: [nil])

    query = "plain" |> select([r], r.x) |> where([r], r.x == true) |> normalize
    assert_query(query, query_order: %{x: true})

    query = "plain" |> select([r], r.x) |> where([r], r.x == false) |> normalize
    assert_query(query, query_order: %{x: false})

    query = "plain" |> select([r], r.x) |> where([r], r.x == "abc") |> normalize
    assert_query(query, query_order: %{x: "abc"})

    query = "plain" |> select([r], r.x) |> where([r], r.x == 123) |> normalize
    assert_query(query, query_order: %{x: 123})

    query = "plain" |> select([r], r.x) |> where([r], r.x == 123.0) |> normalize
    assert_query(query, query_order: %{x: 123.0})
  end

  test "nested expressions" do
    z = 123
    query = from(r in Model, [])
                      |> where([r], r.x > 0 and (r.y > ^(-z)) or true) |> normalize
    assert_query(query, query_order:
                 %{"$or": [%{"$and": [%{x: %{"$gt": 0}}, %{y: %{"$gt": -123}}]}, true]})
  end

  test "binary ops" do
    query = Model |> where([r], r.x == 2) |> normalize
    assert_query(query, query_order: %{x: 2})

    query = Model |> where([r], r.x != 2) |> normalize
    assert_query(query, query_order: %{x: %{"$ne": 2}})

    query = Model |> where([r], r.x <= 2) |> normalize
    assert_query(query, query_order: %{x: %{"$lte": 2}})

    query = Model |> where([r], r.x >= 2) |> normalize
    assert_query(query, query_order: %{x: %{"$gte": 2}})

    query = Model |> where([r], r.x < 2) |> normalize
    assert_query(query, query_order: %{x: %{"$lt": 2}})

    query = Model |> where([r], r.x > 2) |> normalize
    assert_query(query, query_order: %{x: %{"$gt": 2}})
  end

  test "bool ops" do
    query = Model |> where([], true and false) |> normalize
    assert_query(query, query_order: %{"$and": [true, false]})

    query = Model |> where([], true or false) |> normalize
    assert_query(query, query_order: %{"$or": [true, false]})

    query = Model |> where([r], not (r.x > 0) and not (r.x < 5)) |> normalize
    assert_query(query, query_order:
                 %{"$and": [%{"$not": %{x: %{"$gt": 0}}}, %{"$not": %{x: %{"$lt": 5}}}]})
  end

  test "in expression" do
    query = Model |> where([e], e.x in []) |> normalize
    assert_query(query, query_order: %{x: %{"$in": []}})

    query = Model |> where([e], e.x in ^[1, 2, 3]) |> normalize
    assert_query(query, query_order: %{x: %{"$in": [1, 2, 3]}})

    query = Model |> where([e], e.x in [1, ^2, 3]) |> normalize
    assert_query(query, query_order: %{x: %{"$in": [1, 2, 3]}})

    assert_raise ArgumentError, fn ->
      Model |> where([e], 1 in ^[]) |> normalize
    end

    assert_raise ArgumentError, fn ->
      Model |> where([e], e.x in [1, e.x, 3]) |> normalize
    end
  end

  test "having" do
    assert_raise Ecto.QueryError, fn ->
      Model |> having([p], p.x == p.x) |> normalize
    end
  end

  test "group by" do
    assert_raise Ecto.QueryError, fn ->
      Model |> group_by([r], r.x) |> select([r], r.x) |> normalize
    end
  end
end
