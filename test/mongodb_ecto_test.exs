defmodule MongodbEctoTest do
  use ExUnit.Case

  test "from_bson" do
    bson = {:key, 123, :other_key, "value"}
    document = %{key: 123, other_key: "value"}
    assert MongodbEcto.from_bson(bson) == document
  end

  test "to_bson" do
    document = %{key: 123, other_key: "value"}
    bson = {:key, 123, :other_key, "value"}
    assert MongodbEcto.to_bson(document) == bson
  end
end
