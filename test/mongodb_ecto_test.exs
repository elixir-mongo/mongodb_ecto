defmodule Mongo.EctoTest do
  use ExUnit.Case
  doctest Mongo.Ecto

  test "greets the world" do
    assert Mongo.Ecto.hello() == :world
  end
end
