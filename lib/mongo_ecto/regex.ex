defmodule Mongo.Ecto.Regex do
  @moduledoc """
  An Ecto type to represent MongoDB's Regex objects

  Both MongoDB and Elixir use PCRE (Pearl Compatible Regular Expressions),
  so the syntax is similar for both.

  ## Using in queries

  MongoDB supports two ways to use regexes.

  One is to use regex object:

      regex = Mongo.Ecto.Helpers.regex("elixir", "i")
      from p in Post, where: fragment(title: ^regex)

  The other one is to use the `$regex` operator:

      from p in Post, where: fragment(title: ["$regex": "elixir", "$options": "i"])

  For more information about the differences between the two syntaxes please
  refer to the MongoDB documentation:
  http://docs.mongodb.org/manual/reference/operator/query/regex/#syntax-restrictions

  ## Options

    * `i` - case insensitive
    * `m` - causes ^ and $ to mark the beginning and end of each line
    * `x` - whitespace characters are ignored except when escaped
      and allow # to delimit comments, requires using `$regex` operator together
      with the `$option` operator
    * `s` - causes dot to match newlines, requires using `$regex` operator together
      with the `$option` operator
  """

  @behaviour Ecto.Type

  defstruct BSON.Regex |> Map.from_struct() |> Enum.to_list()
  @type t :: %__MODULE__{pattern: String.t(), options: String.t()}

  @doc """
  The Ecto primitive type.
  """
  def type, do: :any

  @doc """
  Casts to database format
  """
  def cast(%BSON.Regex{} = js), do: {:ok, js}
  def cast(_), do: :error

  @doc """
  Converts a `Mongo.Ecto.Regex` into `BSON.Regex`
  """
  def dump(%__MODULE__{} = js), do: {:ok, Map.put(js, :__struct__, BSON.Regex)}
  def dump(_), do: :error

  @doc """
  Converts a `BSON.Regex` into `Mongo.Ecto.Regex`
  """
  def load(%BSON.Regex{} = js), do: {:ok, Map.put(js, :__struct__, __MODULE__)}
  def load(_), do: :error
end
