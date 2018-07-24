defmodule Mongo.Ecto.ChangeMap do
  @moduledoc """
  An Ecto type to represent a partial update of a nested document

  ## Using in queries

      change = Mongo.Ecto.Helpers.change_map("name", "name")
      MyRepo.update_all Post, set: [author: change]
  """

  @behaviour Ecto.Type

  defstruct [:field, :value]
  @type t :: %__MODULE__{field: String.t(), value: term}

  @doc """
  The Ecto primitive type
  """
  def type, do: :any

  @doc """
  Casts to database format
  """
  def cast(%__MODULE__{} = value), do: {:ok, value}
  def cast(_), do: :error

  @doc """
  Converts to a database format
  """
  def dump(%__MODULE__{} = value), do: {:ok, value}
  def dump(_), do: :error

  @doc """
  Change is not a value - it can't be loaded
  """
  def load(_), do: :error
end

defmodule Mongo.Ecto.ChangeArray do
  @moduledoc """
  An Ecto type to represent a partial update of a nested document

  ## Using in queries

      change = Mongo.Ecto.Helpers.change_array("0.name", "name")
      MyRepo.update_all Post, set: [authors: change]
  """

  @behaviour Ecto.Type

  defstruct [:field, :value]
  @type t :: %__MODULE__{field: String.t(), value: term}

  @doc """
  The Ecto primitive type
  """
  def type, do: {:array, :any}

  @doc """
  Casts to database format
  """
  def cast(%__MODULE__{} = value), do: {:ok, value}
  def cast(_), do: :error

  @doc """
  Converts to a database format
  """
  def dump(%__MODULE__{} = value), do: {:ok, value}
  def dump(_), do: :error

  @doc """
  Change is not a value - it can't be loaded
  """
  def load(_), do: :error
end
