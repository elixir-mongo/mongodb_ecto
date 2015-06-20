defmodule Mongo.Ecto.JavaScript do
  @moduledoc """
  An Ecto type to represent MongoDB's JavaScript functions
  """

  @behaviour Ecto.Type

  def type, do: :any

  def cast(%BSON.JavaScript{} = js), do: {:ok, js}
  def cast(_), do: :error

  def dump(%BSON.JavaScript{} = js), do: {:ok, js}
  def dump(_), do: :error

  def load(%BSON.JavaScript{} = js), do: {:ok, js}
  def load(_), do: :error
end
