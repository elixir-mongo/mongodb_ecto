defmodule Mongo.Ecto.JavaScript do
  @moduledoc """
  An Ecto type to represent MongoDB's JavaScript functions

  ## Using in queries

      javascript = Mongo.Ecto.Helpers.javascript("this.visits === count", count: 1)
      from p in Post, where: ^javascript
  """

  @behaviour Ecto.Type

  defstruct BSON.JavaScript |> Map.from_struct |> Enum.to_list
  @type t :: %BSON.JavaScript{code: String.t, scope: %{}}

  @doc """
  The Ecto primitive type.
  """
  def type, do: :any

  @doc """
  Casts to database format
  """
  def cast(%BSON.JavaScript{} = js),
    do: {:ok, js}
  def cast(_),
    do: :error

  @doc """
  Converts a `Mongo.Ecto.JavaScript` into `BSON.JavaScript`
  """
  def dump(%__MODULE__{} = js),
    do: {:ok, Map.put(js, :__struct__, BSON.JavaScript)}
  def dump(_),
    do: :error

  @doc """
  Converts a `BSON.JavaScript` into `Mongo.Ecto.JavaScript`
  """
  def load(%BSON.JavaScript{} = js),
    do: {:ok, Map.put(js, :__struct__, __MODULE__)}
  def load(_),
    do: :error
end
