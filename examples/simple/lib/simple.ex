defmodule Simple.App do
  use Application

  def start(_type, _args) do
    import Supervisor.Spec

    tree = [worker(Simple.Repo, [])]

    opts = [name: Simple.Sup, strategy: :one_for_one]
    Supervisor.start_link(tree, opts)
  end
end

defmodule Simple.Repo do
  use Ecto.Repo, otp_app: :simple
end

defmodule Weather do
  use Ecto.Schema

  @primary_key {:id, :binary_id, autogenerate: true}

  schema "weather" do
    field :city, :string
    field :temp_lo, :integer
    field :temp_hi, :integer
    field :prcp, :float, default: 0.0
    timestamps
  end
end

defmodule Simple do
  import Ecto.Query

  def sample_query do
    query =
      from w in Weather,
        where: w.prcp > 0.0 or is_nil(w.prcp),
        select: w

    Simple.Repo.all(query)
  end

  def sample_insert do
    %Weather{}
    |> Ecto.Changeset.change(%{})
    |> Simple.Repo.insert()
  end

  def sample_update do
    {:ok, weather} = sample_insert()

    weather
    |> Ecto.Changeset.change(%{city: "NYC"})
    |> Simple.Repo.update()
  end
end
