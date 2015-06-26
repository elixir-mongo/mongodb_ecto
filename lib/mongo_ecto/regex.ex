defmodule Mongo.Ecto.Regex do
  @moduledoc """
  An Ecto type to represent MongoDB's Regex objects
  """

  @behaviour Ecto.Type

  defstruct BSON.Regex |> Map.from_struct |> Enum.to_list

  def type, do: :any

  def cast(%BSON.Regex{} = js),
    do: {:ok, js}
  def cast(_),
    do: :error

  def dump(%__MODULE__{} = js),
    do: {:ok, Map.put(js, :__struct__, BSON.Regex)}
  def dump(_),
    do: :error

  def load(%BSON.Regex{} = js),
    do: {:ok, Map.put(js, :__struct__, __MODULE__)}
  def load(_),
    do: :error
end
