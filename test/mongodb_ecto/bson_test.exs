defmodule MongodbEcto.BsonTest do
  use ExUnit.Case, async: true

  alias MongodbEcto.Bson

  test "from_bson" do
    bson = {:key, 123, :other_key, "value"}
    document = %{key: 123, other_key: "value"}
    assert Bson.from_bson(bson) == document
  end

  test "to_bson" do
    document = %{key: 123, other_key: "value"}
    bson = {:key, 123, :other_key, "value"}
    assert Bson.to_bson(document) == bson
  end
end
