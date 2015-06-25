defmodule Mongo.Ecto.DecoderTest do
  use ExUnit.Case, async: true

  alias Mongo.Ecto.Decoder

  test "nested documents" do
    id = <<29, 30, 240, 82, 101, 119, 228, 62, 133, 177, 152, 109>>

    doc = %{"field" => "value", "nested" => %{"_id" => %BSON.ObjectId{value: id}}}
    decoded = %{"field" => "value", "nested" => %{"id" => id}}

    assert decoded == Decoder.decode_document(doc, :id)
  end
end
