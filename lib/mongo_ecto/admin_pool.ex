defmodule Mongo.Ecto.AdminPool do
  @moduledoc false

  use Mongo.Pool, name: __MODULE__, adapter: Mongo.Pool.Poolboy
end
