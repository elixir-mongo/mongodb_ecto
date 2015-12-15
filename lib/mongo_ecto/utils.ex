defmodule Mongo.Ecto.Utils do
  @moduledoc false

  # Make sure you use this before is_list/1
  defmacro is_keyword(doc) do
    quote do
      unquote(doc) |> hd |> tuple_size == 2
    end
  end

  defmacro is_literal(value) do
    quote do
      is_atom(unquote(value)) or is_number(unquote(value)) or is_binary(unquote(value))
    end
  end
end
