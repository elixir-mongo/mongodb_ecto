defmodule Mongo.Ecto.BsonTest do
  use ExUnit.Case, async: true

  alias Mongo.Ecto.Bson

  test "from_bson" do
    bson = {:key, 123, :other_key, "value"}
    document = %{key: 123, other_key: "value"}
    assert Bson.from_bson(bson, nil) == document
  end

  test "to_bson" do
    document = %{key: 123, other_key: "value"}
    bson = {:key, 123, :other_key, "value"}
    assert Bson.to_bson(document, nil) == bson
  end
end
