defmodule Mongo.Ecto.ObjectID do
  @moduledoc """
  An Ecto type to represent MongoDB's ObjectIDs

  Represented as hex-encoded binaries of 24 characters.
  """

  @behaviour Ecto.Type

  @doc """
  The Ecto primitive type.
  """
  def type, do: :object_id

  @doc """
  Casts to valid hex-encoded binary
  """
  def cast(<<_::24-binary>> = hex), do: {:ok, hex}
  def cast(_), do: :error


  @doc """
  Converts to a format acceptable for the database
  """
  def dump(<<_::24-binary>> = hex) do
    case Base.decode16(hex, case: :mixed) do
      {:ok, value} -> {:ok, %Ecto.Query.Tagged{type: :object_id, value: value}}
      :error       -> :error
    end
  end
  def dump(_), do: :error


  @doc """
  Converts from the format returned from the database
  """
  def load(binary) when is_binary(binary), do: {:ok, Base.encode16(binary, case: :lower)}
  def load(_), do: :error
end
