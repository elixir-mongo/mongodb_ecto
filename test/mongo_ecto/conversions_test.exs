defmodule Mongo.Ecto.ConversionsTest do
  use ExUnit.Case, async: true

  alias Mongo.Ecto.Conversions

  describe "from_ecto_pk/2" do
    test "renames the primary key to :_id at the top level" do
      assert {:ok, encoded} = Conversions.from_ecto_pk(%{id: "abc", title: "t"}, :id)

      assert Map.new(encoded) == %{_id: "abc", title: "t"}
    end

    test "does not rename :id inside a nested map when the parent pk is :id" do
      input = %{
        id: "abc",
        title: "outer",
        jurisdiction: %{id: "MD", title: "Maryland"}
      }

      assert {:ok, encoded} = Conversions.from_ecto_pk(input, :id)

      encoded_map = Map.new(encoded)
      assert encoded_map[:_id] == "abc"
      refute Map.has_key?(encoded_map, :id)

      # The nested :id field must be passed through unchanged — it belongs to
      # the embedded sub-document, not the parent schema.
      assert Map.new(encoded_map[:jurisdiction]) == %{id: "MD", title: "Maryland"}
    end

    test "does not rename :id inside a list of nested maps" do
      input = %{id: "abc", items: [%{id: "one"}, %{id: "two"}]}

      assert {:ok, encoded} = Conversions.from_ecto_pk(input, :id)

      encoded_map = Map.new(encoded)
      assert encoded_map[:_id] == "abc"

      assert Enum.map(encoded_map[:items], &Map.new/1) == [
               %{id: "one"},
               %{id: "two"}
             ]
    end

    test "leaves nested maps alone when the pk is nil" do
      input = %{id: "abc", jurisdiction: %{id: "MD"}}

      assert {:ok, encoded} = Conversions.from_ecto_pk(input, nil)

      encoded_map = Map.new(encoded)
      assert encoded_map[:id] == "abc"
      assert Map.new(encoded_map[:jurisdiction]) == %{id: "MD"}
    end
  end

  describe "inject_params/3" do
    test "renames the primary key to :_id only at the top level" do
      input = %{
        id: "abc",
        jurisdiction: %{id: "MD", title: "Maryland"}
      }

      assert {:ok, encoded} = Conversions.inject_params(input, {}, :id)

      encoded_map = Map.new(encoded)
      assert encoded_map[:_id] == "abc"
      refute Map.has_key?(encoded_map, :id)

      assert Map.new(encoded_map[:jurisdiction]) == %{id: "MD", title: "Maryland"}
    end
  end
end
