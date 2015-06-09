defmodule Mongo.EctoTest do
  use ExUnit.Case

  alias Ecto.Integration.TestRepo
  alias Mongo.Ecto, as: ME

  setup do
    ME.truncate(TestRepo)
    :ok
  end

  test "create_collection/3 and list_collections/1" do
    ME.command(TestRepo, create: "posts")

    assert ME.list_collections(TestRepo) |> Enum.member?(%{name: "posts", options: %{}})

    ME.command(TestRepo, create: "users", capped: true, size: 5 * 1024)

    users_collection = %{name: "users", options: %{capped: true, size: 5 * 1024}}
    assert ME.list_collections(TestRepo) |> Enum.member?(users_collection)
  end

  test "truncate/1" do
    ME.command(TestRepo, create: "test")
    # There is always system collection
    assert 2 == ME.list_collections(TestRepo) |> Enum.count

    ME.truncate(TestRepo)
    assert 1 == ME.list_collections(TestRepo) |> Enum.count
  end
end
