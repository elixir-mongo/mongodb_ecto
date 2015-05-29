defmodule MongodbEcto.QueryTest do
  use ExUnit.Case

  alias MongodbEcto.Query
  import Ecto.Query

  defmodule Model do
    use Ecto.Model

    schema "model" do
      field :x, :integer
      field :y, :integer
    end
  end

  defp normalize(query) do
    {query, _params} = Ecto.Query.Planner.prepare(query, [])
    Ecto.Query.Planner.normalize(query, [], [])
  end

  test "bare model" do
    query = Model |> from |> normalize
    assert Query.all(query) == {"model", %{}, %{}, 0}
  end

  test "where" do
    query = Model |> where([r], r.x == 42) |> where([r], r.y != 43) |> normalize
    assert Query.all(query) == {"model", %{x: %{"$eq": 42}, y: %{"$ne": 43}}, %{}, 0}
  end
end
