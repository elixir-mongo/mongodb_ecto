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
    assert Query.all(query) == {"model", {}, {}, 0}
  end
end
