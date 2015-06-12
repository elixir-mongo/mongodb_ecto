# defmodule Mongo.Ecto.QueryTest do
#   use ExUnit.Case, async: true

#   alias Mongo.Ecto.Query
#   import Ecto.Query

#   defmodule Model do
#     use Ecto.Model

#     schema "model" do
#       field :x, :integer
#       field :y, :integer
#     end
#   end

#   defp normalize(query) do
#     {query, params} = Ecto.Query.Planner.prepare(query, [], %{})
#     {Ecto.Query.Planner.normalize(query, [], []), params}
#   end

#   test "bare model" do
#     {query, params} = Model |> from |> normalize
#     assert Query.all(query, params) == {"model", Model, %{}, %{}, 0, 0}
#   end

#   test "from without model" do
#     {query, params} = "posts" |> select([r], r.x) |> normalize
#     assert Query.all(query, params) == {"posts", nil, %{}, %{x: true}, 0, 0}
#   end

#   test "where" do
#     {query, params} = Model |> where([r], r.x == 42) |> where([r], r.y != 43)
#                       |> select([r], r.x) |> normalize
#     selector = %{x: 42, y: %{"$ne": 43}}
#     assert Query.all(query, params) == {"model", Model, selector, %{x: true}, 0, 0}

#     {query, params} = Model |> where([r], not (r.x == 42)) |> normalize
#     assert Query.all(query, params) == {"model", Model, %{"$not": %{x: 42}}, %{}, 0, 0}
#   end

#   test "select" do
#     {query, params} = Model |> select([r], {r.x, r.y}) |> normalize
#     assert Query.all(query, params) == {"model", Model, %{}, %{x: true, y: true}, 0, 0}

#     {query, params} = Model |> select([r], [r.x, r.y]) |> normalize
#     assert Query.all(query, params) == {"model", Model, %{}, %{x: true, y: true}, 0, 0}
#   end

#   test "order by" do
#     {query, params} = Model |> order_by([r], r.x) |> select([r], r.x) |> normalize
#     selector = %{"$query": %{}, "$orderby": %{x: 1}}
#     assert Query.all(query, params) == {"model", Model, selector, %{x: true}, 0, 0}

#     {query, params} = Model |> order_by([r], [r.x, r.y]) |> select([r], r.x) |> normalize
#     selector = %{"$query": %{}, "$orderby": %{x: 1, y: 1}}
#     assert Query.all(query, params) == {"model", Model, selector, %{x: true}, 0, 0}

#     {query, params} = Model
#                       |> order_by([r], [asc: r.x, desc: r.y]) |> select([r], r.x) |> normalize
#     selector = %{"$query": %{}, "$orderby": %{x: 1, y: -1}}
#     assert Query.all(query, params) == {"model", Model, selector, %{x: true}, 0, 0}

#     {query, params} = Model |> order_by([r], []) |> select([r], r.x) |> normalize
#     selector = %{"$query": %{}, "$orderby": %{}}
#     assert Query.all(query, params) == {"model", Model, selector, %{x: true}, 0, 0}
#   end

#   test "limit and offset" do
#     {query, params} = Model |> limit([r], 3) |> normalize
#     assert Query.all(query, params) == {"model", Model, %{}, %{}, 0, 3}

#     {query, params} = Model |> offset([r], 5) |> normalize
#     assert Query.all(query, params) == {"model", Model, %{}, %{}, 5, 0}

#     {query, params} = Model |> offset([r], 5) |> limit([r], 3) |> normalize
#     assert Query.all(query, params) == {"model", Model, %{}, %{}, 5, 3}
#   end

#   test "lock" do
#     {query, params} = Model |> lock("FOR SHARE NOWAIT") |> normalize

#     assert_raise Ecto.QueryError, fn ->
#       Query.all(query, params)
#     end
#   end

#   test "distinct" do
#     {query, params} = Model |> distinct([r], r.x) |> select([r], {r.x, r.y}) |> normalize
#     assert_raise Ecto.QueryError, fn ->
#       Query.all(query, params)
#     end

#     {query, params} = Model |> distinct(true) |> select([r], {r.x, r.y}) |> normalize
#     assert_raise Ecto.QueryError, fn ->
#       Query.all(query, params)
#     end
#   end

#   test "is_nil" do
#     {query, params} = Model |> where([r], is_nil(r.x)) |> normalize
#     assert Query.all(query, params) == {"model", Model, %{x: nil}, %{}, 0, 0}

#     {query, params} = Model |> where([r], not is_nil(r.x)) |> normalize
#     assert Query.all(query, params) == {"model", Model, %{x: %{"$neq": nil}}, %{}, 0, 0}
#   end

#   test "literals" do
#     # TODO how to check nil?
#     # {query, params} = Model |> select([], nil) |> normalize
#     # assert Query.all(query, params) == ~s{SELECT NULL FROM "model" AS m0}

#     {query, params} = "plain" |> select([r], r.x) |> where([r], r.x == true) |> normalize
#     assert Query.all(query, params) == {"plain", nil, %{x: true}, %{x: true}, 0, 0}

#     {query, params} = "plain" |> select([r], r.x) |> where([r], r.x == false) |> normalize
#     assert Query.all(query, params) == {"plain", nil, %{x: false}, %{x: true}, 0, 0}

#     {query, params} = "plain" |> select([r], r.x) |> where([r], r.x == "abc") |> normalize
#     assert Query.all(query, params) == {"plain", nil, %{x: "abc"}, %{x: true}, 0, 0}

#     {query, params} = "plain" |> select([r], r.x) |> where([r], r.x == 123) |> normalize
#     assert Query.all(query, params) == {"plain", nil, %{x: 123}, %{x: true}, 0, 0}

#     {query, params} = "plain" |> select([r], r.x) |> where([r], r.x == 123.0) |> normalize
#     assert Query.all(query, params) == {"plain", nil, %{x: 123.0}, %{x: true}, 0, 0}
#   end

#   test "nested expressions" do
#     z = 123
#     {query, params} = from(r in Model, [])
#                       |> where([r], r.x > 0 and (r.y > ^(-z)) or true) |> normalize
#     selector = %{"$or": [{:"$and", [x: %{"$gt": 0}, y: %{"$gt": -123}]}, true]}
#     assert Query.all(query, params) == {"model", Model, selector, %{}, 0, 0}
#   end
# end
