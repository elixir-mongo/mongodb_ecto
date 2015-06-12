defmodule Mongo.EctoTest do
  use ExUnit.Case

  alias Ecto.Integration.TestRepo
  alias Mongo.Ecto, as: ME

  setup do
    ME.truncate(TestRepo)
    :ok
  end

  test "command/3" do
    assert ME.command(TestRepo, ping: 1) == {:ok, [%{"ok" => 1.0}]}
  end

  test "list_collections/1" do
    ME.command(TestRepo, create: "posts")
    collection = %{"name" => "posts", "options" => %{}}

    assert ME.list_collections(TestRepo) |> Enum.member?(collection)

    ME.command(TestRepo, create: "users", capped: true, size: 5 * 1024)

    collection = %{"name" => "users", "options" => %{"capped" => true, "size" => 5 * 1024}}
    assert ME.list_collections(TestRepo) |> Enum.member?(collection)
  end

  test "truncate/1" do
    ME.command(TestRepo, create: "test")
    # There is always system collection
    assert 2 == ME.list_collections(TestRepo) |> Enum.count

    ME.truncate(TestRepo)
    assert 1 == ME.list_collections(TestRepo) |> Enum.count
  end
end
