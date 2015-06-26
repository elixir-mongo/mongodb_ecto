defmodule Mongo.Ecto.Helpers do
  @moduledoc """
  Defines helpers to ease working with MongoDB in models or other places
  where you work with them. You'd probably want to import it.
  """

  @doc """
  Allows using inline JavaScript in queries in where clauses and inserting it
  as a value to the database.

  ## Usage in queries

      from p in Post,
        where: ^javascript("this.value === value", value: 1)
  """
  def javascript(code, scope \\ []) do
    %Mongo.Ecto.JavaScript{code: code, scope: Enum.into(scope, %{})}
  end

  @doc """
  Creates proper regex object that can be passed to the database.

  ## Usage in queries

      from p in Post,
        where: fragment(title: ^regex("elixir", "i"))
  """
  def regex(pattern, options \\ "") do
    %Mongo.Ecto.Regex{pattern: pattern, options: options}
  end
end
