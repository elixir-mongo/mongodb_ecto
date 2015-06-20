defmodule Mongo.Ecto.JavaScript do
  @moduledoc """
  An Ecto type to represent MongoDB's JavaScript functions
  """

  @behaviour Ecto.Type

  defstruct BSON.JavaScript |> Map.from_struct |> Enum.to_list

  def type, do: :any

  def cast(%BSON.JavaScript{} = js),
    do: {:ok, js}
  def cast(_),
    do: :error

  def dump(%__MODULE__{} = js),
    do: {:ok, Map.put(js, :__struct__, BSON.JavaScript)}
  def dump(_),
    do: :error

  def load(%BSON.JavaScript{} = js),
    do: {:ok, Map.put(js, :__struct__, __MODULE__)}
  def load(_),
    do: :error
end
