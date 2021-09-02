defmodule Schema do
  use Ecto.Schema

  @primary_key {:id, Ecto.UUID, autogenerate: true}
  schema "schema" do
    field :a, :string

    timestamps()
  end
end

defmodule Repo do
  use Ecto.Repo,
    otp_app: :mongodb_ecto,
    adapter: Mongo.Ecto
end
