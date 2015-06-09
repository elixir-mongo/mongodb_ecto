defmodule Mongo.Ecto.ObjectID do
  @moduledoc """
  An Ecto type to represent MongoDB's ObjectIDs
  """

  @behaviour Ecto.Type

  def type, do: :object_id

  def cast(<<_::24-binary>> = hex), do: {:ok, hex}
  def cast(_), do: :error

  def dump(<<_::24-binary>> = hex) do
    case Base.decode16(hex, case: :mixed) do
      {:ok, value} -> {:ok, %Ecto.Query.Tagged{type: :object_id, value: value}}
      :error       -> :error
    end
  end

  def load(binary), do: {:ok, Base.encode16(binary, case: :lower)}
end
