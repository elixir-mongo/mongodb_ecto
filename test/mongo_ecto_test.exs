defmodule Mongo.EctoTest do
  use ExUnit.Case

  alias Ecto.Integration.TestRepo
  alias Mongo.Ecto, as: ME

  setup do
    ME.truncate(TestRepo)
    :ok
  end

  test "create_collection/3 and list_collections/1" do
    ME.create_collection(TestRepo, "posts")

    assert ME.list_collections(TestRepo) |> Enum.member?(%{name: "posts", options: %{}})

    Mongo.Ecto.create_collection(TestRepo, "users", capped: true, size: 5 * 1024)

    users_collection = %{name: "users", options: %{capped: true, size: 5 * 1024}}
    assert ME.list_collections(TestRepo) |> Enum.member?(users_collection)
  end

  test "truncate/1" do
    ME.create_collection(TestRepo, "test")
    # There is always system collection
    assert 2 == ME.list_collections(TestRepo) |> Enum.count

    ME.truncate(TestRepo)
    assert 1 == ME.list_collections(TestRepo) |> Enum.count
  end

  test "drop_collection/2" do
    ME.create_collection(TestRepo, "test")
    assert 2 = ME.list_collections(TestRepo) |> Enum.count

    ME.drop_collection(TestRepo, "test")
    assert 1 = ME.list_collections(TestRepo) |> Enum.count

    assert {:error, _} = ME.drop_collection(TestRepo, "invalid")
  end
end
